package server

import (
	"bytes"
	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils/json"
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
	if len(oldVal) > 0 {
		err := json.Unmarshal(oldVal, &oldV)
		if err != nil {
			logrus.Warnf("old %s not valid, %s", oldVal, err)
			return nil, false
		}
		if oldV.UpdatedAt < 0 {
			logrus.Warnf("old %s not valid", oldVal)
			return nil, false
		}
	}

	existV := UpdatedAtValue{}
	if len(existVal) > 0 {
		err := json.Unmarshal(existVal, &existV)
		if err != nil {
			logrus.Warnf("exist %s not valid, %s", existVal, err)
			return nil, false
		}
		if existV.UpdatedAt < 0 {
			logrus.Warnf("exist %s not valid", existVal)
			return nil, false
		}
	}

	if oldV.UpdatedAt < existV.UpdatedAt {
		logrus.Warnf("old %s < exist %s, new %s", oldVal, existVal, newVal)
		return nil, false
	}

	if len(newVal) == 0 {
		return nil, true
	}

	newV := UpdatedAtValue{}
	err := json.Unmarshal(newVal, &newV)
	if err != nil {
		logrus.Warnf("new %s not valid, %s", newVal, err)
		return nil, false
	}

	if newV.UpdatedAt <= 0 {
		logrus.Warnf("new %s not valid", newVal)
		return nil, false
	}

	if oldV.UpdatedAt > newV.UpdatedAt {
		logrus.Warnf("old %s > new %s, exist %s", oldVal, newVal, existVal)
		return nil, false
	}

	if existV.UpdatedAt > newV.UpdatedAt {
		logrus.Warnf("exist %s > new %s, old %s", existVal, newVal, oldVal)
		return nil, false
	}

	if newV.Deleted {
		return nil, true
	}
	return newVal, true
}
