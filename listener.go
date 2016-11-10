package main

import (
	"errors"
	"net"
	"time"
)

// stoppabelListener sets TCP keep-alive timeout on accepted
// connections and waits on stopc message
type stoppableListener struct {
	*net.TCPListener
	stopc <-chan struct{}
}

func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
}

func (ln stoppableListener) Accept() (net.Conn, error) {
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)

	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}

		connc <- tc
	}()

	select {
	case <-ln.stopc:
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Second)
		return tc, nil
	}
}
