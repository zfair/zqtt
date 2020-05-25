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

// Handle a upcoming connection.
func (p *tcpServer) Handle(clientConn net.Conn) {
	p.server.logger.Debug("TCP: new client", zap.String("remoteAddr", clientConn.RemoteAddr().String()))

	conn, err := newConn(p.server, clientConn)
	if err != nil {
		p.server.logger.Error("newConn error", zap.String("remoteAddr", clientConn.RemoteAddr().String()), zap.Error(err))
		return
	}

	p.conns.Store(conn.LUID(), conn)
	err = conn.IOLoop(p.server.ctx)
	if err != nil {
		p.server.logger.Error(
			"client err",
			zap.String("remoteAddr", clientConn.RemoteAddr().String()),
			zap.Error(err),
		)
	}

	p.conns.Delete(conn.LUID())
}

// CloseAll closes all connections.
func (p *tcpServer) CloseAll() {
	p.conns.Range(func(k, v interface{}) bool {
		_ = v.(*Conn).Close()
		return true
	})
}

// TCPHandler is the interface for handling TCP connections.
type TCPHandler interface {
	Handle(net.Conn)
}

// TCPServer creates a new TCP server.
func TCPServer(listener net.Listener, handler TCPHandler, logger *zap.Logger) error {
	logger.Info("TCPServer listening", zap.String("addr", listener.Addr().String()))

	var wg sync.WaitGroup

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Temporary() {
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

	// Waits until all handler goroutines finished.
	wg.Wait()

	logger.Info("TCPServer closing", zap.String("addr", listener.Addr().String()))

	return nil
}
