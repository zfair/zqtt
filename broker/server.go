package broker

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zfair/zqtt/internal/config"
	"github.com/zfair/zqtt/internal/util"
	"go.uber.org/zap"
)

type Server struct {
	connections int64 // The number of currently open connections.

	config atomic.Value

	logger *zap.Logger

	startTime time.Time
	exitChan  chan int

	tcpServer   *tcpServer
	tcpListener net.Listener

	waitGroup util.WaitGroupWrapper
}

func (s *Server) incConnections() {
	atomic.AddInt64(&s.connections, 1)
}

func (s *Server) decConnections() {
	atomic.AddInt64(&s.connections, -1)
}

func (s *Server) getCfg() *config.Config {
	return s.config.Load().(*config.Config)
}

func (s *Server) swapCfg(config *config.Config) {
	s.config.Store(config)
}

func NewServer(config *config.Config) (*Server, error) {

	var err error

	if config.Logger == nil {
		logger, err := zap.NewDevelopment()
		if err != nil {
			return nil, err
		}
		config.Logger = logger
	}

	s := &Server{
		logger:    config.Logger,
		startTime: time.Now(),

		exitChan: make(chan int),
	}

	s.swapCfg(config)

	s.tcpServer = &tcpServer{}
	s.tcpListener, err = net.Listen("tcp", config.TCPAddress)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Server) Main() error {
	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				s.logger.Fatal("Main exit error", zap.Error(err))
			}
			exitCh <- err
		})
	}

	s.tcpServer.server = s
	s.waitGroup.Wrap(func() {
		exitFunc(TCPServer(s.tcpListener, s.tcpServer, s.logger))
	})

	err := <-exitCh
	return err
}

func (s *Server) Exit() {
	if s.tcpListener != nil {
		s.tcpListener.Close()
	}
	if s.tcpServer != nil {
		s.tcpServer.CloseAll()
	}

	close(s.exitChan)
	s.waitGroup.Wait()
}
