package broker

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zfair/zqtt/src/config"
	"github.com/zfair/zqtt/src/internal/topic"
	"github.com/zfair/zqtt/src/internal/util"
	"go.uber.org/zap"
)

// Server that backs the broker.
type Server struct {
	connCount int64

	config atomic.Value

	ctx context.Context

	subTrie *topic.SubTrie // The subscription matching trie.

	logger *zap.Logger

	startTime time.Time
	exitChan  chan int

	tcpServer   *tcpServer
	tcpListener net.Listener

	waitGroup util.WaitGroupWrapper
}

func (s *Server) incrConnCount() {
	atomic.AddInt64(&s.connCount, 1)
}

func (s *Server) decrConnCount() {
	atomic.AddInt64(&s.connCount, -1)
}

func (s *Server) getCfg() *config.Config {
	return s.config.Load().(*config.Config)
}

func (s *Server) swapCfg(config *config.Config) {
	s.config.Store(config)
}

// NewServer creates a new server.
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
		ctx:       context.Background(),
		startTime: time.Now(),

		exitChan: make(chan int),
	}

	s.swapCfg(config)
	s.subTrie = topic.NewSubTrie()

	s.tcpServer = &tcpServer{}
	s.tcpListener, err = net.Listen("tcp", config.TCPAddress)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// Start the server.
func (s *Server) Start() error {
	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				s.logger.Fatal("Start error", zap.Error(err))
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

// Exit terminates the server.
func (s *Server) Exit() {
	if s.tcpListener != nil {
		_ = s.tcpListener.Close()
	}
	if s.tcpServer != nil {
		s.tcpServer.CloseAll()
	}

	close(s.exitChan)
	s.waitGroup.Wait()
}
