package server

import (
	"context"
	"fmt"
	"github.com/DeanThompson/ginpprof"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/middleware"
	"gitlab.s.upyun.com/platform/tikv-proxy/store"
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
	store  *store.Store
	log    *logrus.Entry
}

func NewServer(conf *config.Config) (*Server, error) {
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

	s, err := store.NewStore(conf)
	if err != nil {
		return nil, err
	}
	return &Server{
		server: server,
		router: router,
		conf:   conf,
		store:  s,
		log:    logrus.WithFields(logrus.Fields{"worker": "server"}),
	}, nil
}

func (s *Server) registerRoutes() error {
	//if gin.IsDebugging() {
	//	url := ginSwagger.URL("/swagger/doc.json")
	//	s.router.GET("/swagger/*any",
	//		ginSwagger.WrapHandler(swaggerFiles.Handler, url))
	//}
	if s.conf.EnableTracing {
		trace.AuthRequest = func(req *http.Request) (any, sensitive bool) {
			return true, true
		}

		s.router.GET("/debug/requests", gin.WrapF(trace.Traces))
		s.router.GET("/debug/events", gin.WrapF(trace.Events))

		ginpprof.Wrap(s.router)
		s.router.Use(middleware.SetTrace())
	}

	api := s.router.Group(path.Join("/api", version.API))
	api.GET("/meta/*key", s.Get)
	api.PUT("/meta/*key", s.CheckAndPut)
	api.GET("/list/", s.List)
	return nil
}

func (s *Server) Start() {
	err := s.registerRoutes()
	if err != nil {
		s.log.Errorf("register routes err, %s", err)
		return
	}
	err = s.server.ListenAndServe()
	if err != nil {
		s.log.Errorf("listen err, %s", err)
		return
	}
}

func (s *Server) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.server.Shutdown(ctx)
	if err != nil {
		logrus.Errorf("shutdown failed %s", err)
	}
	err = s.store.Close()
	if err != nil {
		logrus.Errorf("store close failed %s", err)
	}
}