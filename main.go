package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"golang.org/x/net/context"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)

	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)

	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	// the key-value http handler will propose updates to raft
	serveHttpKVAPI(kvs, *kvport, confChangeC, errorC)
}

type raftNode struct {
	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- *string           // entries committed to log(k,v)
	errorC      chan<- error             // errors from raft serrion

	id          int      // client id for raft serrsion
	peers       []string // raft peer URLS
	join        bool     // node is joining an existing cluster
	waldir      string   // path to WAL directory
	snapdir     string   // path to snapshot directory
	getSnapshot func() ([]byte, error)
	lastIndex   uint64 // indix of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{}
	httpstopc chan struct{}
	httpdonec chan struct{}
}

var defaultSnapCount uint64 = 10000

func newRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *string, <-chan error, <-chan *snap.Snapshotter) {

	commitC := make(chan *string)
	errorC := make(chan error)

	rc := raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		waldir:      fmt.Sprintf("node-%d", id),
		snapdir:     fmt.Sprintf("node-%d-snap", id),
		getSnapshot: getSnapshot,
		raftStorage: raft.NewMemoryStorage(),
		snapCount:   defaultSnapCount,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
	}

	go rc.startRaft()

	return commitC, errorC, rc.snapshotterReady
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}

	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}

	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}

	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) []raftpb.Entry {
	var nents []raftpb.Entry
	if len(ents) == 0 {
		return nents
	}

	firstIdx := ents[0].Index

	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry [%d] should <= progress.appliedIndex[%d] 1", firstIdx, rc.appliedIndex)
	}

	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}

	return nents
}

func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				break
			}

			s := string(ents[i].Data)

			select {
			case rc.commitC <- &s:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)

			rc.node.ApplyConfChange(cc)

			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
				}

				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit update appliedIndex
		rc.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rc.lastIndex {
			select {
			case rc.commitC <- nil:
			case <-rc.stopc:
				return false
			}
		}
	}

	return true
}

// returns a WAl ready for reading
func (rc raftNode) openWAL() *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("cannot create dir for wal %v", err)
		}

		w, err := wal.Create(rc.waldir, nil)
		if err != nil {
			log.Fatalf("create wal error %v", err)
		}

		w.Close()
	}

	w, err := wal.Open(rc.waldir, walpb.Snapshot{})
	if err != nil {
		log.Fatalf("loading wal error %v", err)
	}

	return w
}

// replays WAL entries into the raft instance
func (rc *raftNode) replayWAL() *wal.WAL {
	w := rc.openWAL()

	_, st, ents, err := w.ReadAll()

	if err != nil {
		log.Fatalf("replayWAL: failed to read WAL %v", err)
	}

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)

	// send nil once lastIndex is published so client konw commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		rc.commitC <- nil
	}

	rc.raftStorage.SetHardState(st)
	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) startRaft() {
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0755); err != nil {
			log.Fatalf("startRaft: cannot create dir for snapshot %v", err)
		}
	}

	rc.snapshotter = snap.New(rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter

	oldwal := wal.Exist(rc.waldir)
	rc.wal = rc.replayWAL()

	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	c := raft.Config{
		ID:              uint64(rc.id),
		ElectionTick:    10,
		HeartbeatTick:   2,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	if oldwal {
		rc.node = raft.RestartNode(&c)
	} else {
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}

		rc.node = raft.StartNode(&c, startPeers)
	}

	ss := &stats.ServerStats{}
	ss.Initialize()

	rc.transport = &rafthttp.Transport{
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: ss,
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()

	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	go rc.serveRaft()

	go rc.serveChannels()
}

// closes http closes all channels and stops rafts
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing Snapshot at index", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index >= rc.appliedIndex {
		log.Fatalf("publishSnapshot: snapshot index [%d] should > progress.appliedIndex [%d] + 1", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}

	rc.commitC <- nil // trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *raftNode) maybeTriggerSnapshot() {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	log.Printf("maybeTriggerSnapshot: start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}

	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		log.Panic(err)
	}

	if err := rc.saveSnap(snap); err != nil {
		log.Panic(err)
	}

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}

	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("maybeTriggerSnapshot: Compact log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

func (rc *raftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}

	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		var confChangeCount uint64 = 0

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					rc.node.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount += 1
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}

		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		case rd := <-rc.node.Ready():
			rc.wal.Save(rd.HardState, rd.Entries)

			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}

			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}

			rc.maybeTriggerSnapshot()
			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("serveRaft: failed parsing URL %v", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("serveRaft: failed to listen rafthttp %v", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("serveRaft: failed to serve rafthttp", err)
	}

	close(rc.httpstopc)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}

func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
