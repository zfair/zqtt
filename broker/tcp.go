package broker

import (
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type tcpServer struct {
	server *Server
	conns  sync.Map
}

func (p *tcpServer) Handle(clientConn net.Conn) {
	p.server.logger.Info("TCP: new client", zap.String("remoteAddr", clientConn.RemoteAddr().String()))

	conn, err := newConn(p.server, clientConn)
	if err != nil {
		p.server.logger.Error("newConn error", zap.String("remoteAddr", clientConn.RemoteAddr().String()), zap.Error(err))
		return
	}

	p.conns.Store(conn.LUID(), conn)
	err = conn.IOLoop()
	if err != nil {
		p.server.logger.Error(
			"client err",
			zap.String("remoteAddr", clientConn.RemoteAddr().String()),
			zap.Error(err),
		)
	}

	p.conns.Delete(conn.LUID())
}

func (p *tcpServer) CloseAll() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(*Conn).Close()
		return true
	})
}

type TCPHandler interface {
	Handle(net.Conn)
}

func TCPServer(listener net.Listener, handler TCPHandler, logger *zap.Logger) error {
	logger.Info("TCPServer listening", zap.String("addr", listener.Addr().String()))

	var wg sync.WaitGroup

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				logger.Warn("TCPServer temporary Accept() failure", zap.Error(err))
				runtime.Gosched()
				continue
			}
			if !strings.Contains(err.Error(), "use of closed network connection") {
				return errors.Errorf("listener.Accept() error - %s", err)
			}
			break
		}

		wg.Add(1)
		go func() {
			handler.Handle(clientConn)
			wg.Done()
		}()
	}

	// wait to return until all handler goroutines complete
	wg.Wait()

	logger.Info("TCPServer closing", zap.String("addr", listener.Addr().String()))

	return nil
}
