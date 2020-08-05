package broker

import (
	"net/http"
	"time"

	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type QueryMessageV1Request struct {
}

type httpServer struct {
	server       *Server
	addr         string
	tlsEnabled   bool
	tlsRequired  bool
	router       *gin.Engine
	httpListener *http.Server
}

func newHTTPServer(
	server *Server,
) (*httpServer, error) {
	s := &httpServer{
		server: server,
	}

	router := gin.New()
	router.Use(ginzap.Ginzap(server.logger, time.RFC3339, true))
	router.Use(ginzap.RecoveryWithZap(server.logger, true))

	s.RegisterAPIV1Restful(router)
	s.router = router

	s.addr = server.getCfg().HTTPAddress
	s.httpListener = &http.Server{
		Addr:    s.addr,
		Handler: s.router,
	}

	return s, nil
}

func (s *httpServer) RegisterAPIV1Restful(
	router *gin.Engine,
) {
	v1 := router.Group("v1")
	v1.GET("message", s.QueryMessageV1)
}

func (s *httpServer) QueryMessageV1(
	c *gin.Context,
) {
	c.JSON(http.StatusOK, gin.H{
		"Hello": "World",
	})
}

func (s *httpServer) CloseAll() {
	s.httpListener.Close()
}

func HTTPServer(s *httpServer) error {
	s.server.logger.Info(
		"HTTPServer listening",
		zap.String("addr", s.addr),
	)
	err := s.httpListener.ListenAndServe()
	if err != nil {
		if err == http.ErrServerClosed {
			s.server.logger.Info(
				"HTTPServer closing",
				zap.String("addr", s.addr),
			)
		} else {
			s.server.logger.Error(
				"Server closed unexpect",
				zap.Error(err),
			)
			return err
		}
	}
	return nil
}
