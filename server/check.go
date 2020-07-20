package server

import (
	"bytes"
	"encoding/json"
)

// old == exist
func ExactCheck(oldVal, newVal, existVal []byte) bool {
	if len(oldVal) != len(existVal) {
		return false
	}
	if !bytes.Equal(oldVal, existVal) {
		return false
	}
	return true
}

type UpdatedAtValue struct {
	UpdatedAt int64 `json:"updated_at"`
}

// new.updated_at > exist.updated_at
func TimestampCheck(oldVal, newVal, existVal []byte) bool {
	newV := UpdatedAtValue{}
	err := json.Unmarshal(newVal, &newV)
	if err != nil {
		return false
	}

	if newV.UpdatedAt <= 0 {
		return false
	}

	existV := UpdatedAtValue{}
	err = json.Unmarshal(existVal, &existV)
	if err != nil {
		return false
	}

	if newV.UpdatedAt >= existV.UpdatedAt {
		return true
	}
	return false
}
