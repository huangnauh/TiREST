package utils

import (
	"bytes"
	"sync"
)

var bufPool sync.Pool

func GetBuf() *bytes.Buffer {
	buf := bufPool.Get()
	if buf == nil {
		return &bytes.Buffer{}
	}
	return buf.(*bytes.Buffer)
}

func GiveBuf(buf *bytes.Buffer) {
	buf.Reset()
	bufPool.Put(buf)
}
