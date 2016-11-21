package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"golang.org/x/net/context"

	events "github.com/docker/go-events"
	"github.com/upccup/zoro/src/api"
	"github.com/upccup/zoro/src/raft"
	"github.com/upccup/zoro/src/store/boltdb"

	"github.com/boltdb/bolt"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	listen := flag.String("listen", ":5001", "server listen addr")
	id := flag.Int("id", 1, "node ID")
	flag.Parse()

	dbName := fmt.Sprintf("boltdb-%d.db", *id)
	db, err := bolt.Open(dbName, 0644, nil)
	if err != nil {
		log.Fatal(err)
	}

	boltdbStore, err := boltdb.NewBoltdbStore(db)
	if err != nil {
		log.Fatal(err)
	}

	_, raftNode := raft.NewNode(*id, strings.Split(*cluster, ","), boltdbStore)

	leadershipCh, cancel := raftNode.SubscribeLeaderShip()
	defer cancel()

	go handleLeadershipEvents(context.TODO(), leadershipCh)

	api := api.Api{
		Node:  raftNode,
		Store: boltdbStore,
	}

	server := http.Server{
		Addr:           *listen,
		Handler:        api.ApiRouter(),
		MaxHeaderBytes: 1 << 20,
	}

	err = server.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}

func handleLeadershipEvents(ctx context.Context, leadershipCh chan events.Event) {
	for {
		select {
		case leadershipEvent := <-leadershipCh:
			// TODO lock it and if manager stop return
			newState := leadershipEvent.(raft.LeadershipState)

			if newState == raft.IsLeader {
				fmt.Println("Now i am a leader !!!!!")
			} else if newState == raft.IsFollower {
				fmt.Println("Now i am a follower !!!!!")
			}
		case <-ctx.Done():
			return
		}
	}
}
