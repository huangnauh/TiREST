package server

import (
	"encoding/base64"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"gitlab.s.upyun.com/platform/tikv-proxy/model"
	"gitlab.s.upyun.com/platform/tikv-proxy/store"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils"
	"gitlab.s.upyun.com/platform/tikv-proxy/xerror"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

func (s *Server) checkKey(key string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(key[1:])
	if err != nil {
		s.log.Errorf("decode key %s err: %s", key, err)
		return nil, err
	}
	return decoded, nil
}

func (s *Server) Get(c *gin.Context) {
	keyStr := c.Param("key")
	key, err := s.checkKey(keyStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid key"})
		return
	}

	opts := store.NoOption
	if s.conf.Server.ReplicaRead {
		opts = store.ReplicaReadOption
	}
	v, err := s.store.Get(key, opts)
	if err == xerror.ErrNotExists {
		c.Writer.WriteHeader(http.StatusNotFound)
	} else if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	} else {
		c.Header("Content-Length", strconv.Itoa(len(v)))
		c.Data(http.StatusOK, "application/octet-stream", v)
	}
}

func (s *Server) CheckAndPut(c *gin.Context) {
	keyStr := c.Param("key")
	key, err := s.checkKey(keyStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid key"})
		return
	}

	entry, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		s.log.Errorf("read body failed: %s", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err = s.store.CheckAndPut(key, entry)
	if err == xerror.ErrCheckAndSetFailed {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	} else if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

func (s *Server) List(c *gin.Context) {
	l := &model.List{}
	if err := c.ShouldBindHeader(&l); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if strings.Compare(l.Start, l.End) >= 0 {
		s.log.Errorf("list start %s > end %s", l.Start, l.End)
		c.JSON(http.StatusBadRequest, gin.H{"error": "start end invalid"})
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

	keyEntry, err := s.store.List(utils.S2B(l.Start), utils.S2B(l.End), l.Limit, opts)
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
	if err := c.ShouldBindHeader(&l); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if strings.Compare(l.Start, l.End) >= 0 {
		s.log.Errorf("list start %s > end %s", l.Start, l.End)
		c.JSON(http.StatusBadRequest, gin.H{"error": "start end invalid"})
		return
	}

	if l.Limit <= 0 || l.Limit > 10000 {
		l.Limit = 10000
	}

	if l.Unsafe {
		go func() {
			s.store.UnsafeDelete(utils.S2B(l.Start), utils.S2B(l.End))
		}()
		c.Status(http.StatusNoContent)
		return
	}

	go func() {
		count := 0
		for {
			deleted, err := s.store.BatchDelete(utils.S2B(l.Start), utils.S2B(l.End), l.Limit)
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
	err := s.store.Health()
	if err != nil {
		s.log.Errorf("not health, %s", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}
