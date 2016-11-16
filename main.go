package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/upccup/zoro/src/api"
	"github.com/upccup/zoro/src/raft"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	listen := flag.String("listen", ":5001", "server listen addr")
	id := flag.Int("id", 1, "node ID")
	//kvport := flag.Int("port", 9121, "key-value server port")
	flag.Parse()

	// raft provides a commit stream for the proposals from the http api
	var kvs *raft.Kvstore
	getSnapshot := func() ([]byte, error) { return kvs.GetSnapshot() }
	_, _, _, raftNode := raft.NewNode(*id, strings.Split(*cluster, ","), getSnapshot)

	//kvs = raft.NewKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	// the key-value http handler will propose updates to raft
	//go raft.ServeHttpKVAPI(kvs, *kvport, confChangeC, errorC)

	api := api.Api{
		Node: raftNode,
	}

	server := http.Server{
		Addr:           *listen,
		Handler:        api.ApiRouter(),
		MaxHeaderBytes: 1 << 20,
	}

	err := server.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}
