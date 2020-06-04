package middleware

import (
	"github.com/gin-gonic/gin"
	"golang.org/x/net/trace"
	"strings"
)

type traceError struct {
	*gin.Error
}

func (e traceError) String() string {
	return e.Err.Error()
}

func methodFamily(m string) string {
	if i := strings.LastIndex(m, "/"); i >= 0 {
		m = m[i+1:]
	}
	if i := strings.LastIndex(m, "."); i >= 0 {
		m = m[:i]
	}
	return m
}

func SetTrace() gin.HandlerFunc {
	return func(c *gin.Context) {
		tr := trace.New(methodFamily(c.HandlerName()), c.Request.URL.Path)
		c.Next()
		if len(c.Errors) != 0 {
			for _, err := range c.Errors {
				tr.LazyLog(traceError{err}, false)
			}
			tr.SetError()
		}
		tr.Finish()
	}
}
