package server

import (
	"bytes"
	"encoding/base64"
	"github.com/gin-gonic/gin"
	"gitlab.s.upyun.com/platform/tikv-proxy/model"
	"gitlab.s.upyun.com/platform/tikv-proxy/store"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils/json"
	"gitlab.s.upyun.com/platform/tikv-proxy/xerror"
	"io/ioutil"
	"net/http"
	"strconv"
)

func (s *Server) checkBase64(key string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		s.log.Errorf("decode key %s err: %s", key, err)
		return nil, err
	}
	return decoded, nil
}

func (s *Server) Get(c *gin.Context) {
	keyStr := c.Param("key")
	key, err := s.checkBase64(keyStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid key"})
		return
	}

	l := &model.Meta{}
	if err := c.ShouldBindHeader(&l); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	opts := store.NoOption
	if s.conf.Server.ReplicaRead {
		opts.ReplicaRead = true
	}
	if l.Secondary != "" {
		opts.Secondary = utils.S2B(l.Secondary)
	}

	v, err := s.store.Get(key, opts)
	if err == xerror.ErrNotExists {
		c.Status(http.StatusNotFound)
	} else if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	} else {
		if v.Secondary {
			c.Header("X-Secondary", "true")
		}
		c.Header("Content-Length", strconv.Itoa(len(v.Value)))
		c.Data(http.StatusOK, "application/octet-stream", v.Value)
	}
}

func (s *Server) CheckAndPut(c *gin.Context) {
	keyStr := c.Param("key")
	key, err := s.checkBase64(keyStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid key"})
		return
	}

	l := &model.Meta{}
	if err := c.ShouldBindHeader(&l); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var check store.CheckFunc
	if !l.Exact {
		check = TimestampCheck
	} else {
		check = ExactCheck
	}

	entry, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		s.log.Errorf("read body failed: %s", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err = s.store.CheckAndPut(key, entry, check)
	if err == xerror.ErrCheckAndSetFailed {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	} else if err == xerror.ErrAlreadyExists {
		c.Status(http.StatusOK)
		return
	} else if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

func (s *Server) getRangeFromList(l *model.List) ([]byte, []byte, error) {
	var start, end []byte
	var err error
	if !l.Raw {
		start, err = s.checkBase64(l.Start)
		if err != nil {
			return nil, nil, err
		}

		end, err = s.checkBase64(l.End)
		if err != nil {
			return nil, nil, err
		}
	} else {
		start = utils.S2B(l.Start)
		end = utils.S2B(l.End)
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
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
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

	opts := store.NoOption
	if l.KeyOnly {
		opts = store.KeyOnlyOption
	}

	if l.Reverse {
		opts.Reverse = true
	}

	keyEntry, err := s.store.List(start, end, l.Limit, opts)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	} else {
		jsonBytes, err := json.Marshal(keyEntry)
		if err != nil {
			s.log.Errorf("list failed, %s", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.Header("Content-Length", strconv.Itoa(len(jsonBytes)))
		c.Data(http.StatusOK, "application/json", jsonBytes)
	}
}

func (s *Server) AsyncBatchDelete(c *gin.Context) {
	l := &model.List{}
	err := c.ShouldBindHeader(&l)
	if err != nil {
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
		c.JSON(http.StatusBadRequest, gin.H{"error": "is closed"})
		return
	}

	err := s.store.Health()
	if err != nil {
		s.log.Errorf("not health, %s", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}
