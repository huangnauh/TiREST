package server

import (
	"bytes"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"gitlab.s.upyun.com/platform/tikv-proxy/middleware"
	"gitlab.s.upyun.com/platform/tikv-proxy/model"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils/json"
	"gitlab.s.upyun.com/platform/tikv-proxy/xerror"
)

func (s *Server) Get(c *gin.Context) {
	l := &model.Meta{}
	if err := c.ShouldBindHeader(&l); err != nil {
		s.log.Errorf("bind header, err %s", err)
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	keyStr := c.Param("key")
	key, err := EncodeMetaKey(keyStr, l.Raw)
	if err != nil {
		s.log.Errorf("check key %s, err %s", keyStr, err)
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid key"})
		return
	}

	opts := DefaultGetOption()
	if s.conf.Server.ReplicaRead {
		opts.ReplicaRead = true
	}
	if l.Secondary != "" {
		secondary, err := EncodeMetaKey(l.Secondary, l.Raw)
		if err != nil {
			s.log.Errorf("check secondary key %s, err %s", l.Secondary, err)
			c.Set(middleware.HttpMessage, err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid secondary"})
			return
		}
		opts.Secondary = secondary
	}

	v, err := s.store.Get(key, opts)
	if err == xerror.ErrNotExists {
		c.Status(http.StatusNotFound)
	} else if err != nil {
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	} else {
		if v.Secondary {
			c.Header("X-Secondary", "true")
		}
		c.Header("Content-Length", strconv.Itoa(len(v.Value)))
		c.Data(http.StatusOK, "application/octet-stream", v.Value)
	}
}

func (s *Server) UnsafeDelete(c *gin.Context) {
	l := &model.Meta{}
	if err := c.ShouldBindHeader(&l); err != nil {
		s.log.Errorf("bind header, err %s", err)
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	keyStr := c.Param("key")
	key, err := EncodeMetaKey(keyStr, l.Raw)
	if err != nil {
		s.log.Errorf("check key %s, err %s", keyStr, err)
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid key"})
		return
	}

	err = s.store.UnsafePut(key, nil)
	if err != nil {
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	} else {
		c.Status(http.StatusNoContent)
	}
}

func (s *Server) UnsafePut(c *gin.Context) {
	l := &model.Meta{}
	if err := c.ShouldBindHeader(&l); err != nil {
		s.log.Errorf("bind header, err %s", err)
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	keyStr := c.Param("key")
	key, err := EncodeMetaKey(keyStr, l.Raw)
	if err != nil {
		s.log.Errorf("check key %s, err %s", keyStr, err)
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid key"})
		return
	}

	val, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		s.log.Errorf("read body failed: %s", err)
		c.Set(middleware.HttpMessage, err.Error())
		if e, ok := err.(net.Error); ok && e.Timeout() {
			c.JSON(499, gin.H{"error": err.Error()})
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}
		return
	}

	err = s.store.UnsafePut(key, val)
	if err != nil {
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	} else {
		c.Status(http.StatusNoContent)
	}
}

func (s *Server) CheckAndPut(c *gin.Context) {
	l := &model.Meta{}
	if err := c.ShouldBindHeader(&l); err != nil {
		s.log.Errorf("bind header, err %s", err)
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	keyStr := c.Param("key")
	key, err := EncodeMetaKey(keyStr, l.Raw)
	if err != nil {
		s.log.Errorf("check key %s, err %s", keyStr, err)
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid key"})
		return
	}

	opts := DefaultCheckOption()
	if l.Exact {
		opts.Check = ExactCheck
	}

	entry, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		s.log.Errorf("read body failed: %s", err)
		c.Set(middleware.HttpMessage, err.Error())
		if e, ok := err.(net.Error); ok && e.Timeout() {
			c.JSON(499, gin.H{"error": err.Error()})
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}
		return
	}

	err = s.store.CheckAndPut(key, entry, opts)
	if err == xerror.ErrCheckAndSetFailed {
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	} else if err == xerror.ErrAlreadyExists {
		c.Status(http.StatusOK)
		return
	} else if err != nil {
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

func (s *Server) getRangeFromList(l *model.List) ([]byte, []byte, error) {
	start, err := EncodeMetaKey(l.Start, l.Raw)
	if err != nil {
		return nil, nil, err
	}
	end, err := EncodeMetaKey(l.End, l.Raw)
	if err != nil {
		return nil, nil, err
	}

	if bytes.Compare(start, end) >= 0 {
		s.log.Errorf("list start %s > end %s", l.Start, l.End)
		return nil, nil, xerror.ErrListKVInvalid
	}
	return start, end, nil
}

func (s *Server) List(c *gin.Context) {
	l := &model.List{}
	err := c.ShouldBindHeader(&l)
	if err != nil {
		s.log.Errorf("bind header, err %s", err)
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	start, end, err := s.getRangeFromList(l)
	if err != nil {
		s.log.Errorf("list invalid, err %s", err)
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if l.Limit <= 0 || l.Limit > 10000 {
		l.Limit = 10000
	}
	s.log.Debugf("list (%s-%s), limit %d, reverse %t", start, end, l.Limit, l.Reverse)

	opts := DefaultListOption()
	if l.KeyOnly {
		opts.KeyOnly = true
	}

	if l.Reverse {
		opts.Reverse = true
	}

	keyEntry, err := s.store.List(start, end, l.Limit, opts)
	if err != nil {
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	jsonBytes, err := json.Marshal(keyEntry)
	if err != nil {
		s.log.Errorf("list failed, %s", err)
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.Header("Content-Length", strconv.Itoa(len(jsonBytes)))
	c.Data(http.StatusOK, "application/json", jsonBytes)
}

func (s *Server) AsyncBatchDelete(c *gin.Context) {
	l := &model.List{}
	err := c.ShouldBindHeader(&l)
	if err != nil {
		s.log.Errorf("bind header, err %s", err)
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if l.Reverse {
		c.JSON(http.StatusBadRequest, gin.H{"error": xerror.ErrNotSupported})
		return
	}

	start, end, err := s.getRangeFromList(l)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if l.Limit <= 0 || l.Limit > 10000 {
		l.Limit = 10000
	}

	if l.Unsafe {
		go func() {
			s.store.UnsafeDelete(start, end)
		}()
		c.Status(http.StatusNoContent)
		return
	}

	go func() {
		count := 0
		lastKey := start
		var err error
		for {
			deleted := 0
			lastKey, deleted, err = s.store.BatchDelete(lastKey, end, l.Limit)
			if err != nil {
				s.log.Errorf("list (%s-%s), deleted %d, err: %s", l.Start, l.End, count, err)
				return
			}
			s.log.Infof("list (%s-%s), deleted %d", l.Start, l.End, count)
			if deleted < l.Limit {
				return
			}
			count += deleted
		}
	}()
	c.Status(http.StatusNoContent)
}

func (s *Server) GetConfig(c *gin.Context) {
	c.Render(http.StatusOK, utils.TOML{Data: s.conf})
}

func (s *Server) Health(c *gin.Context) {
	if s.closed {
		c.Set(middleware.HttpMessage, "closed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "is closed"})
		return
	}

	err := s.store.Health()
	if err != nil {
		s.log.Errorf("not health, %s", err)
		c.Set(middleware.HttpMessage, err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}
