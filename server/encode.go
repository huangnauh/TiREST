package server

import (
	"encoding/base64"
	"gitlab.s.upyun.com/platform/tikv-proxy/xerror"
)

const (
	MetaType byte = 0x00
)

func EncodeMetaKey(s string, raw bool) ([]byte, error) {
	if !raw {
		return encodeKey(MetaType, s)
	} else {
		return encodeRawKey(MetaType, s)
	}
}

func encodeRawKey(t byte, s string) ([]byte, error) {
	dbuf := make([]byte, 1+len(s))
	dbuf[0] = t
	copy(dbuf[1:], s)
	return dbuf, nil
}

func DecodeMetaKey(key []byte) ([]byte, error) {
	if len(key) == 1 {
		return nil, xerror.ErrNotExists
	}
	return key[1:], nil
}

func ItemFunc(key, val []byte) ([]byte, []byte, error) {
	key, err := DecodeMetaKey(key)
	if err != nil {
		return nil, nil, err
	}
	return key[1:], val, nil
}

func encodeKey(t byte, s string) ([]byte, error) {
	dbuf := make([]byte, 1+base64.RawURLEncoding.DecodedLen(len(s)))
	dbuf[0] = t
	n, err := base64.RawURLEncoding.Decode(dbuf[1:], []byte(s))
	return dbuf[:n+1], err
}

func decodeBase64(key string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

func encodeBase64(key []byte) string {
	return base64.RawURLEncoding.EncodeToString(key)
}
