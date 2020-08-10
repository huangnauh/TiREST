package server

import (
	"context"
	"fmt"
	"github.com/DeanThompson/ginpprof"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	closed bool
}

func NewServer(conf *config.Config) (*Server, error) {
	mode := conf.HttpServerMode()
	gin.SetMode(mode)
	router := gin.New()
	router.Use(middleware.SetAccessLog())
	if mode != gin.DebugMode {
		router.Use(gin.Recovery())
	}

	server := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", conf.Server.HttpHost, conf.Server.HttpPort),
		Handler:           router,
		ReadTimeout:       conf.Server.ReadTimeout.Duration,
		ReadHeaderTimeout: conf.Server.ReadHeaderTimeout.Duration,
		WriteTimeout:      conf.Server.WriteTimeout.Duration,
		IdleTimeout:       conf.Server.IdleTimeout.Duration,
	}

	s, err := store.NewStore(conf)
	if err != nil {
		return nil, err
	}

	ser := &Server{
		server: server,
		router: router,
		conf:   conf,
		store:  s,
		log:    logrus.WithFields(logrus.Fields{"worker": "server"}),
	}

	err = ser.registerRoutes()
	if err != nil {
		ser.log.Errorf("register routes err, %s", err)
		return nil, err
	}
	return ser, nil
}

func HandleNoRoute(c *gin.Context) {
	c.Status(http.StatusNotImplemented)
	return
}

var (
	ApiRoute    = path.Join("/api", version.API)
	UnsafeRoute = "unsafe"
)

func prometheusHandler() http.Handler {
	return promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{ErrorLog: logrus.StandardLogger()})
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
	s.router.GET("/metrics", gin.WrapH(prometheusHandler()))

	s.router.NoRoute(HandleNoRoute)
	api := s.router.Group(ApiRoute)
	api.GET("/meta/:key", s.Get)
	api.PUT("/meta/:key", s.CheckAndPut)
	api.POST("/meta/:key", s.CheckAndPut)
	api.DELETE("/list/", s.AsyncBatchDelete)
	api.DELETE("/list", s.AsyncBatchDelete)
	api.GET("/list/", s.List)
	api.GET("/list", s.List)
	api.GET("/config", s.GetConfig)
	api.GET("/health", s.Health)

	unsafe := api.Group(UnsafeRoute)
	unsafe.DELETE("/meta/:key", s.UnsafeDelete)
	unsafe.PUT("/meta/:key", s.UnsafePut)
	unsafe.POST("/meta/:key", s.UnsafePut)
	return nil
}

func (s *Server) Start() {
	go func() {
		s.store.Open()
	}()

	s.log.Infof("Serving HTTP on %s port %d", s.conf.Server.HttpHost, s.conf.Server.HttpPort)
	err := s.server.ListenAndServe()
	if err != nil {
		return
	}
}

func (s *Server) Close() {
	if s.closed {
		return
	}
	s.closed = true
	// waiting health check done
	time.Sleep(s.conf.Server.SleepBeforeClose.Duration)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s.log.Infof("shutdown server")
	err := s.server.Shutdown(ctx)
	if err != nil {
		logrus.Errorf("shutdown failed %s", err)
	}
	middleware.CloseAccessLog()
	s.log.Infof("shutdown store")
	err = s.store.Close()
	if err != nil {
		logrus.Errorf("store close failed %s", err)
	}
}
