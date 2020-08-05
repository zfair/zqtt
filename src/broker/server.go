package broker

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zfair/zqtt/src/config"
	"github.com/zfair/zqtt/src/internal/provider/storage"
	"github.com/zfair/zqtt/src/internal/provider/storage/postgres"
	"github.com/zfair/zqtt/src/internal/topic"
	"github.com/zfair/zqtt/src/internal/util"
	"go.uber.org/zap"
)

// Server that backs the broker.
type Server struct {
	connCount int64

	cfg atomic.Value

	ctx context.Context

	subTrie *topic.SubTrie // The subscription matching trie.

	MStore storage.MStorage
	SStore storage.SStorage

	logger *zap.Logger

	startTime time.Time
	exitChan  chan int

	tcpServer   *tcpServer
	tcpListener net.Listener

	httpServer *httpServer

	waitGroup util.WaitGroupWrapper
}

func (s *Server) incrConnCount() {
	atomic.AddInt64(&s.connCount, 1)
}

func (s *Server) decrConnCount() {
	atomic.AddInt64(&s.connCount, -1)
}

func (s *Server) getCfg() *config.Config {
	return s.cfg.Load().(*config.Config)
}

func (s *Server) swapCfg(config *config.Config) {
	s.cfg.Store(config)
}

// NewServer creates a new server.
func NewServer(cfg *config.Config) (*Server, error) {
	var err error

	if cfg.Logger == nil {
		logger, err := zap.NewDevelopment()
		if err != nil {
			return nil, err
		}
		cfg.Logger = logger
	}

	s := &Server{
		logger:    cfg.Logger,
		ctx:       context.Background(),
		startTime: time.Now(),

		exitChan: make(chan int),
	}

	s.swapCfg(cfg)
	s.subTrie = topic.NewSubTrie()

	MStore, err := config.LoadProvider(
		s.ctx,
		cfg.MStorage,
		// register postgres storage
		postgres.NewMStorage(cfg.Logger),
	)
	if err != nil {
		return nil, err
	}

	s.MStore = MStore.(storage.MStorage)

	SStore, err := config.LoadProvider(
		s.ctx,
		cfg.SStorage,
		// register postgres storage
		postgres.NewSStorage(cfg.Logger),
	)
	if err != nil {
		return nil, err
	}

	s.SStore = SStore.(storage.SStorage)

	s.tcpServer = &tcpServer{}
	s.tcpListener, err = net.Listen("tcp", cfg.TCPAddress)
	if err != nil {
		return nil, err
	}

	s.httpServer, err = newHTTPServer(s)
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

	s.waitGroup.Wrap(func() {
		exitFunc(HTTPServer(s.httpServer))
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
	if s.httpServer != nil {
		s.httpServer.CloseAll()
	}

	close(s.exitChan)
	s.waitGroup.Wait()
}
