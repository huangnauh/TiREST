package server

import (
	"context"
	"fmt"
	"github.com/DeanThompson/ginpprof"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/swaggo/gin-swagger"
	"github.com/swaggo/gin-swagger/swaggerFiles"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/middleware"
	"gitlab.s.upyun.com/platform/tikv-proxy/version"
	"golang.org/x/net/trace"
	"net/http"
	"path"
	"time"
)

type Server struct {
	server *http.Server
	router *gin.Engine
	conf   *config.Config
}

func NewServer(conf *config.Config) *Server {
	gin.SetMode(conf.HttpServerMode())
	router := gin.New()
	router.Use(middleware.SetAccessLog(), gin.Recovery())

	server := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", conf.Server.HttpHost, conf.Server.HttpPort),
		Handler:           router,
		ReadTimeout:       conf.Server.ReadTimeout,
		ReadHeaderTimeout: conf.Server.ReadHeaderTimeout,
		WriteTimeout:      conf.Server.WriteTimeout,
		IdleTimeout:       conf.Server.IdleTimeout,
	}
	return &Server{server: server, router: router, conf: conf}
}

func (s *Server) registerRoutes() error {
	if gin.IsDebugging() {
		url := ginSwagger.URL("/swagger/doc.json")
		s.router.GET("/swagger/*any",
			ginSwagger.WrapHandler(swaggerFiles.Handler, url))
	}
	if s.conf.EnableTracing {
		trace.AuthRequest = func(req *http.Request) (any, sensitive bool) {
			return true, true
		}

		s.router.GET("/debug/requests", gin.WrapF(trace.Traces))
		s.router.GET("/debug/events", gin.WrapF(trace.Events))

		ginpprof.Wrap(s.router)
		s.router.Use(middleware.SetTrace())
	}

	_ := s.router.Group(path.Join("/api", version.API))
	return nil
}

func (s *Server) Start(ctx context.Context) error {
	err := s.registerRoutes()
	if err != nil {
		return err
	}
	err = s.server.ListenAndServe()

	if err != nil {
		return err
	}
	return nil
}

func (s *Server) Close(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.server.Shutdown(ctx)
	if err != nil {
		logrus.Errorf("shutdown failed %s", err)
	}
}
