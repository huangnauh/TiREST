package utils

import (
	"github.com/BurntSushi/toml"
	"net/http"
)

type TOML struct {
	Data interface{}
}

var tomlContentType = []string{"application/x-toml; charset=utf-8"}

func (r TOML) Render(w http.ResponseWriter) error {
	r.WriteContentType(w)
	return toml.NewEncoder(w).Encode(r.Data)
}

func (r TOML) WriteContentType(w http.ResponseWriter) {
	header := w.Header()
	if val := header["Content-Type"]; len(val) == 0 {
		header["Content-Type"] = tomlContentType
	}
}
