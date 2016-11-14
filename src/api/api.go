package api

import (
	"github.com/upccup/zoro/src/raft"
)

type Api struct {
	Node *raft.RaftNode
}
