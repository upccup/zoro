package raft

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"github.com/coreos/etcd/snap"
)

// a key-value store backed by raft
type Kvstore struct {
	proposeC    chan<- string
	mu          sync.RWMutex
	kvStore     map[string]string
	snapshotter *snap.Snapshotter
}

type kv struct {
	Key string
	Val string
}

func NewKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *Kvstore {
	s := &Kvstore{proposeC: proposeC, kvStore: make(map[string]string), snapshotter: snapshotter}

	// replay log into key-value map
	s.readCommits(commitC, errorC)

	// read commits from raft into kvstore until error
	go s.readCommits(commitC, errorC)

	return s
}

func (s *Kvstore) Lookup(k string) (string, bool) {
	s.mu.RLock()
	v, ok := s.kvStore[k]
	s.mu.RUnlock()
	return v, ok
}

func (s *Kvstore) Propose(k, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatalf("kv Propose: encoder failed %v", err)
	}

	s.proposeC <- string(buf.Bytes())
}

func (s *Kvstore) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new dara coming or signaled to load snapshot
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}

			if err != nil && err != snap.ErrNoSnapshot {
				log.Panic(err)
			}

			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
		}

		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("readCommits: could not decode messgae %v", err)
		}

		s.mu.Lock()
		s.kvStore[dataKv.Key] = dataKv.Val
		s.mu.Unlock()
	}

	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *Kvstore) GetSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.kvStore)
}

func (s *Kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string

	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}

	s.mu.Lock()
	s.kvStore = store
	s.mu.Unlock()
	return nil
}
