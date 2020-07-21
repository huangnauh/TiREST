package server

import (
	"bytes"
	"encoding/json"
	"github.com/sirupsen/logrus"
)

// old == exist
func ExactCheck(oldVal, newVal, existVal []byte) ([]byte, bool) {
	if len(oldVal) != len(existVal) {
		return nil, false
	}
	if !bytes.Equal(oldVal, existVal) {
		return nil, false
	}
	return newVal, true
}

type UpdatedAtValue struct {
	UpdatedAt int64 `json:"updated_at"`
	Deleted   bool  `json:"deleted"`
}

func TimestampCheck(oldVal, newVal, existVal []byte) ([]byte, bool) {
	oldV := UpdatedAtValue{}
	err := json.Unmarshal(oldVal, &oldV)
	if err != nil {
		return nil, false
	}
	if oldV.UpdatedAt < 0 {
		return nil, false
	}

	existV := UpdatedAtValue{}
	err = json.Unmarshal(existVal, &existV)
	if err != nil {
		return nil, false
	}
	if existV.UpdatedAt < 0 {
		return nil, false
	}

	if oldV.UpdatedAt < existV.UpdatedAt {
		logrus.Warnf("online old %s < exist %s, new %s", oldVal, existVal, newVal)
		return nil, false
	}

	if len(newVal) == 0 {
		return nil, true
	}

	newV := UpdatedAtValue{}
	err = json.Unmarshal(newVal, &newV)
	if err != nil {
		return nil, false
	}

	if newV.UpdatedAt <= 0 {
		return nil, false
	}

	if oldV.UpdatedAt > newV.UpdatedAt {
		logrus.Warnf("online old %s > new %s, exist %s", oldVal, newVal, existVal)
		return nil, false
	}

	if existV.UpdatedAt > newV.UpdatedAt {
		logrus.Warnf("online exist %s > new %s, old %s", existVal, newVal, oldVal)
		return nil, false
	}

	if newV.Deleted {
		return nil, true
	}
	return newVal, true
}
