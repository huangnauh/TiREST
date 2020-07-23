package server

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/stretchr/testify/assert"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/store"
	_ "gitlab.s.upyun.com/platform/tikv-proxy/store/kafka"
	_ "gitlab.s.upyun.com/platform/tikv-proxy/store/newtikv"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils/json"
	"gitlab.s.upyun.com/platform/tikv-proxy/xerror"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
	"time"
)

var (
	pathArg = flag.String("conf", "", "test conf path")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}

func newApiTest() (s *Server) {
	flag.Parse()
	if *pathArg == "" {
		panic("need config file")
		return
	}
	conf, err := config.InitConfig(*pathArg)
	if err != nil {
		panic(err)
	}
	s, err = NewServer(conf)
	if err != nil {
		panic(err)
	}
	s.store.Open()
	time.Sleep(time.Second)
	return
}

func PerformRequest(r http.Handler, method, path string,
	body io.Reader) *httptest.ResponseRecorder {
	req, _ := http.NewRequest(method, path, body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func error2HttpCode(err error) int {
	if err == xerror.ErrCheckAndSetFailed {
		return http.StatusConflict
	} else if err == xerror.ErrAlreadyExists {
		return http.StatusOK
	} else if err != nil {
		return http.StatusBadRequest
	}
	return http.StatusNoContent
}

func TestServer(t *testing.T) {
	t.Parallel()
	s := newApiTest()
	t.Run("server", func(t *testing.T) {
		for _, tt := range checkTestCases {
			tt := tt
			t.Run(tt.Name, func(t *testing.T) {
				t.Parallel()
				b := RandBytes(20)
				key := encodeBase64(b)

				// Not Exist
				result := PerformRequest(s.router, http.MethodGet,
					path.Join(ApiRoute, "meta", key), nil)
				res := result.Result()
				assert.Equal(t, http.StatusNotFound, res.StatusCode)
				err := res.Body.Close()
				assert.Nil(t, err)

				// Put Exist
				body, err := json.Marshal(store.Log{New: utils.B2S(tt.Exist)})
				assert.Nil(t, err)
				result = PerformRequest(s.router, http.MethodPut,
					path.Join(ApiRoute, "meta", key), bytes.NewReader(body))
				res = result.Result()
				assert.Equal(t, true, res.StatusCode < 300, fmt.Sprintf("Code %d", res.StatusCode))
				assert.Equal(t, true, res.StatusCode >= 200, fmt.Sprintf("Code %d", res.StatusCode))
				_, err = ioutil.ReadAll(res.Body)
				assert.Nil(t, err)
				err = res.Body.Close()
				assert.Nil(t, err)

				// Get Exist
				result = PerformRequest(s.router, http.MethodGet,
					path.Join(ApiRoute, "meta", key), nil)
				res = result.Result()
				if len(tt.Exist) == 0 {
					assert.Equal(t, http.StatusNotFound, res.StatusCode)
				} else {
					assert.Equal(t, http.StatusOK, res.StatusCode)
				}

				body, err = ioutil.ReadAll(res.Body)
				assert.Nil(t, err)
				assert.Equal(t, len(tt.Exist), len(body))
				if len(body) > 0 {
					assert.Equal(t, tt.Exist, body)
				}
				err = res.Body.Close()
				assert.Nil(t, err)

				// Check And Put
				body, err = json.Marshal(store.Log{Old: utils.B2S(tt.Old), New: utils.B2S(tt.New)})
				assert.Nil(t, err)
				result = PerformRequest(s.router, http.MethodPut,
					path.Join(ApiRoute, "meta", key), bytes.NewReader(body))
				res = result.Result()
				assert.Equal(t, error2HttpCode(tt.Err), res.StatusCode)
				_, err = ioutil.ReadAll(res.Body)
				assert.Nil(t, err)
				err = res.Body.Close()
				assert.Nil(t, err)

				// Get Output
				val := tt.Output
				if tt.Err != nil {
					val = tt.Exist
				}
				result = PerformRequest(s.router, http.MethodGet,
					path.Join(ApiRoute, "meta", key), nil)
				res = result.Result()
				if len(val) == 0 {
					assert.Equal(t, http.StatusNotFound, res.StatusCode)
				} else {
					assert.Equal(t, http.StatusOK, res.StatusCode)
				}
				body, err = ioutil.ReadAll(res.Body)
				assert.Nil(t, err)
				if len(val) > 0 {
					assert.Equal(t, len(val), len(body))
					if len(body) > 0 {
						assert.Equal(t, val, body)
					}
				}
				err = res.Body.Close()
				assert.Nil(t, err)

				// Clear
				result = PerformRequest(s.router, http.MethodDelete,
					path.Join(ApiRoute, UnsafeRoute, "meta", key), nil)
				res = result.Result()
				assert.Equal(t, http.StatusNoContent, res.StatusCode)
				err = res.Body.Close()
				assert.Nil(t, err)
			})
		}

		// list
	})
	s.Close()
}
