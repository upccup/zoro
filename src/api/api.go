package api

import (
	"github.com/upccup/zoro/src/raft"
	"github.com/upccup/zoro/src/store"
)

type Api struct {
	Node  *raft.Node
	Store store.Store
}
