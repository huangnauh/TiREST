package server

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
	"time"

	"github.com/mozillazg/go-httpheader"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/model"
	"gitlab.s.upyun.com/platform/tikv-proxy/store"
	_ "gitlab.s.upyun.com/platform/tikv-proxy/store/kafka"
	_ "gitlab.s.upyun.com/platform/tikv-proxy/store/newtikv"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils/json"
	"gitlab.s.upyun.com/platform/tikv-proxy/xerror"
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
	}
	logrus.SetLevel(logrus.DebugLevel)
	conf, err := config.InitConfig(*pathArg)
	if err != nil {
		panic(err)
	}
	conf.Store.GCEnable = false
	s, err = NewServer(conf)
	if err != nil {
		panic(err)
	}
	err = s.store.OpenDatabase()
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
	return
}

func PerformRequest(r http.Handler, method, path string,
	headers http.Header, body io.Reader) *httptest.ResponseRecorder {
	req, _ := http.NewRequest(method, path, body)
	req.Header = headers
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

type listTestCase struct {
	Name  string
	Key   []byte
	Value []byte
}

var listTestCases = []listTestCase{
	{"key_1", []byte("\x001111"), []byte(`{"updated_at": 100, "key_1": true}`)},
	{"key_2", []byte("\x002222"), []byte(`{"updated_at": 100, "key_2": true}`)},
}

func reverseListTestCases() []listTestCase {
	listTC := make([]listTestCase, len(listTestCases))
	copy(listTC, listTestCases)
	for i, j := 0, len(listTC)-1; i < j; i, j = i+1, j-1 {
		listTC[i], listTC[j] = listTC[j], listTC[i]
	}
	return listTC
}

var checkConfigCases = []struct {
	Name   string
	Old    []byte
	New    []byte
	CheckOption config.CheckOption
	StatusCode  int
} {
	{"no_check_ok", []byte(`{"updated_at": 100, "old": true}`), []byte(`{"updated_at": 1, "old": true}`), config.NopCheck, 204},
	{"no_check_same", []byte(`{"updated_at": 100, "old": true}`), []byte(`{"updated_at": 100, "old": true}`), config.NopCheck, 204},
	{"exact_check_ok", []byte(`{"updated_at": 100, "old": true}`), []byte(`{"updated_at": 1, "old": true}`), config.ExactCheck, 204},
	{"exact_check_ok", []byte(`{"updated_at": 100, "old": true}`), []byte(`{"updated_at": 100, "old": true}`), config.ExactCheck, 200},
}

func TestServer(t *testing.T) {
	t.Parallel()
	s := newApiTest()
	t.Run("check", func(t *testing.T) {
		for _, tt := range checkConfigCases {
			tt := tt
			t.Run(tt.Name, func(t *testing.T) {
				oldOption := s.conf.Server.CheckOption
				s.conf.Server.CheckOption = tt.CheckOption
				b := RandBytes(20)
				key := encodeBase64(b)

				// Not Exist
				result := PerformRequest(s.router, http.MethodGet,
					path.Join(ApiRoute, "meta", key), nil, nil)
				res := result.Result()
				assert.Equal(t, http.StatusNotFound, res.StatusCode)
				err := res.Body.Close()
				assert.Nil(t, err)

				// Put Old
				body, err := json.Marshal(store.Log{New: utils.B2S(tt.Old)})
				assert.Nil(t, err)
				result = PerformRequest(s.router, http.MethodPut,
					path.Join(ApiRoute, "meta", key), nil, bytes.NewReader(body))
				res = result.Result()
				assert.Equal(t, true, res.StatusCode < 300, fmt.Sprintf("Code %d", res.StatusCode))
				assert.Equal(t, true, res.StatusCode >= 200, fmt.Sprintf("Code %d", res.StatusCode))
				_, err = ioutil.ReadAll(res.Body)
				assert.Nil(t, err)
				err = res.Body.Close()
				assert.Nil(t, err)

				// Put New
				body, err = json.Marshal(store.Log{Old: utils.B2S(tt.Old), New: utils.B2S(tt.New)})
				assert.Nil(t, err)
				result = PerformRequest(s.router, http.MethodPut,
					path.Join(ApiRoute, "meta", key), nil, bytes.NewReader(body))
				res = result.Result()
				assert.Equal(t, tt.StatusCode, res.StatusCode, fmt.Sprintf("Code %d", res.StatusCode))
				_, err = ioutil.ReadAll(res.Body)
				assert.Nil(t, err)
				err = res.Body.Close()
				assert.Nil(t, err)

				s.conf.Server.CheckOption = oldOption
			})
		}
	})

	t.Run("meta", func(t *testing.T) {
		for _, tt := range checkTestCases {
			tt := tt
			t.Run(tt.Name, func(t *testing.T) {
				t.Parallel()
				b := RandBytes(20)
				key := encodeBase64(b)

				// Not Exist
				result := PerformRequest(s.router, http.MethodGet,
					path.Join(ApiRoute, "meta", key), nil, nil)
				res := result.Result()
				assert.Equal(t, http.StatusNotFound, res.StatusCode)
				err := res.Body.Close()
				assert.Nil(t, err)

				// Put Exist
				body, err := json.Marshal(store.Log{New: utils.B2S(tt.Exist)})
				assert.Nil(t, err)
				result = PerformRequest(s.router, http.MethodPut,
					path.Join(ApiRoute, "meta", key), nil, bytes.NewReader(body))
				res = result.Result()
				assert.Equal(t, true, res.StatusCode < 300, fmt.Sprintf("Code %d", res.StatusCode))
				assert.Equal(t, true, res.StatusCode >= 200, fmt.Sprintf("Code %d", res.StatusCode))
				_, err = ioutil.ReadAll(res.Body)
				assert.Nil(t, err)
				err = res.Body.Close()
				assert.Nil(t, err)

				// Get Exist
				result = PerformRequest(s.router, http.MethodGet,
					path.Join(ApiRoute, "meta", key), nil, nil)
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
					path.Join(ApiRoute, "meta", key), nil, bytes.NewReader(body))
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
					path.Join(ApiRoute, "meta", key), nil, nil)
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
					path.Join(ApiRoute, UnsafeRoute, "meta", key), nil, nil)
				res = result.Result()
				assert.Equal(t, http.StatusNoContent, res.StatusCode)
				err = res.Body.Close()
				assert.Nil(t, err)
			})
		}

		// put list test cases
		t.Run("put", func(t *testing.T) {
			for _, tt := range listTestCases {
				tt := tt
				t.Run(tt.Name, func(t *testing.T) {
					t.Parallel()
					key := encodeBase64(tt.Key)
					t.Logf("Key: %s", key)
					body, err := json.Marshal(store.Log{New: utils.B2S(tt.Value)})
					assert.Nil(t, err)
					result := PerformRequest(s.router, http.MethodPut,
						path.Join(ApiRoute, "meta", key), nil, bytes.NewReader(body))
					res := result.Result()
					assert.Equal(t, true, res.StatusCode < 300, fmt.Sprintf("Code %d", res.StatusCode))
					assert.Equal(t, true, res.StatusCode >= 200, fmt.Sprintf("Code %d", res.StatusCode))
					_, err = ioutil.ReadAll(res.Body)
					assert.Nil(t, err)
					err = res.Body.Close()
					assert.Nil(t, err)

					result = PerformRequest(s.router, http.MethodGet,
						path.Join(ApiRoute, "meta", key), nil, nil)
					res = result.Result()
					assert.Equal(t, http.StatusOK, res.StatusCode)
					body, err = ioutil.ReadAll(res.Body)
					assert.Nil(t, err)
					assert.Equal(t, tt.Value, body)
					err = res.Body.Close()
					assert.Nil(t, err)
				})
			}
		})

		// check list test cases
		t.Run("list", func(t *testing.T) {
			start := encodeBase64([]byte("\x00"))
			end := encodeBase64([]byte("\x01"))
			t.Run("normal", func(t *testing.T) {
				t.Parallel()
				l := &model.List{
					Start: start,
					End:   end,
					Limit: 10,
				}
				h, _ := httpheader.Header(l)
				result := PerformRequest(s.router, http.MethodGet,
					path.Join(ApiRoute, "list"), h, nil)
				res := result.Result()
				body, err := ioutil.ReadAll(res.Body)
				assert.Nil(t, err)
				assert.Equal(t, http.StatusOK, res.StatusCode, fmt.Sprintf("body: %s", body))
				items := make([]store.KeyValue, 0)
				err = json.Unmarshal(body, &items)
				assert.Nil(t, err, fmt.Sprintf("body: %s", body))
				for i, item := range items {
					assert.Equal(t, listTestCases[i].Key, []byte(item.Key))
					assert.Equal(t, listTestCases[i].Value, []byte(item.Value))
				}
			})

			t.Run("limit", func(t *testing.T) {
				t.Parallel()
				l := &model.List{
					Start: start,
					End:   end,
					Limit: 1,
				}
				h, _ := httpheader.Header(l)
				result := PerformRequest(s.router, http.MethodGet,
					path.Join(ApiRoute, "list"), h, nil)
				res := result.Result()
				body, err := ioutil.ReadAll(res.Body)
				assert.Nil(t, err, fmt.Sprintf("body: %s", body))
				assert.Equal(t, http.StatusOK, res.StatusCode, fmt.Sprintf("body: %s", body))
				items := make([]store.KeyValue, 0)
				err = json.Unmarshal(body, &items)
				assert.Nil(t, err, fmt.Sprintf("body: %s", body))
				assert.Equal(t, len(items), l.Limit)
				for i := 0; i < l.Limit; i++ {
					assert.Equal(t, listTestCases[i].Key, []byte(items[i].Key))
					assert.Equal(t, listTestCases[i].Value, []byte(items[i].Value))
				}
			})

			t.Run("reverse", func(t *testing.T) {
				t.Parallel()
				l := &model.List{
					Start:   start,
					End:     end,
					Limit:   10,
					Reverse: true,
				}
				h, _ := httpheader.Header(l)
				result := PerformRequest(s.router, http.MethodGet,
					path.Join(ApiRoute, "list"), h, nil)
				res := result.Result()
				body, err := ioutil.ReadAll(res.Body)
				assert.Nil(t, err, fmt.Sprintf("body: %s", body))
				assert.Equal(t, http.StatusOK, res.StatusCode, fmt.Sprintf("body: %s", body))
				items := make([]store.KeyValue, 0)
				err = json.Unmarshal(body, &items)
				assert.Nil(t, err, fmt.Sprintf("body: %s", body))
				//reverse
				reverseList := reverseListTestCases()
				// check
				for i, item := range items {
					assert.Equal(t, reverseList[i].Key, []byte(item.Key))
					assert.Equal(t, reverseList[i].Value, []byte(item.Value))
				}
			})

			t.Run("reverse_limit", func(t *testing.T) {
				t.Parallel()
				l := &model.List{
					Start:   start,
					End:     end,
					Limit:   1,
					Reverse: true,
				}
				h, _ := httpheader.Header(l)
				result := PerformRequest(s.router, http.MethodGet,
					path.Join(ApiRoute, "list"), h, nil)
				res := result.Result()
				body, err := ioutil.ReadAll(res.Body)
				assert.Nil(t, err, fmt.Sprintf("body: %s", body))
				assert.Equal(t, http.StatusOK, res.StatusCode, fmt.Sprintf("body: %s", body))
				items := make([]store.KeyValue, 0)
				err = json.Unmarshal(body, &items)
				assert.Nil(t, err, fmt.Sprintf("body: %s", body))
				assert.Equal(t, len(items), l.Limit, fmt.Sprintf("body: %s", body))
				//reverse
				reverseList := reverseListTestCases()
				// check
				for i := 0; i < l.Limit; i++ {
					logrus.Debugf("item: %v", items[i])
					assert.Equal(t, reverseList[i].Key, []byte(items[i].Key))
					assert.Equal(t, reverseList[i].Value, []byte(items[i].Value))
				}
			})
		})

		// delete list test cases
		t.Run("delete", func(t *testing.T) {
			for _, tt := range listTestCases {
				tt := tt
				t.Run(tt.Name, func(t *testing.T) {
					t.Parallel()
					key := encodeBase64(tt.Key)
					t.Logf("Key: %s", key)
					result := PerformRequest(s.router, http.MethodDelete,
						path.Join(ApiRoute, UnsafeRoute, "meta", key), nil, nil)
					res := result.Result()
					assert.Equal(t, true, res.StatusCode < 300, fmt.Sprintf("Code %d", res.StatusCode))
					assert.Equal(t, true, res.StatusCode >= 200, fmt.Sprintf("Code %d", res.StatusCode))
					_, err := ioutil.ReadAll(res.Body)
					assert.Nil(t, err)
					err = res.Body.Close()
					assert.Nil(t, err)

					result = PerformRequest(s.router, http.MethodGet,
						path.Join(ApiRoute, "meta", key), nil, nil)
					res = result.Result()
					assert.Equal(t, http.StatusNotFound, res.StatusCode)
					err = res.Body.Close()
					assert.Nil(t, err)
				})
			}
		})
	})
	s.Close()
}
