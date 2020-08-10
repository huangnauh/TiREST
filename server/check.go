package server

import (
	"bytes"
	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils/json"
	"gitlab.s.upyun.com/platform/tikv-proxy/xerror"
	"reflect"
)

// old == exist
func ExactCheck(oldVal, newVal, existVal []byte) ([]byte, error) {
	if bytes.Equal(newVal, existVal) {
		return nil, xerror.ErrAlreadyExists
	}

	if len(oldVal) != len(existVal) {
		return nil, xerror.ErrCheckAndSetFailed
	}
	if !bytes.Equal(oldVal, existVal) {
		return nil, xerror.ErrCheckAndSetFailed
	}
	return newVal, nil
}

type UpdatedAtValue struct {
	CreatedAt int64 `json:"create_at"`
	UpdatedAt int64 `json:"updated_at"`
	Deleted   bool  `json:"deleted"`
}

type MetaValue struct {
	Key           string                 `json:"key"`
	BlockSize     int                    `json:"block_size"`
	BlockUUID     string                 `json:"block_uuid"`
	ClusterID     string                 `json:"cluster_id"`
	ContentLength int64                  `json:"content_length"`
	ContentMd5    string                 `json:"content_md5"`
	ContentType   string                 `json:"content_type"`
	CreatedAt     int64                  `json:"create_at"`
	UpdatedAt     int64                  `json:"updated_at"`
	Deleted       bool                   `json:"deleted"`
	Metadata      map[string]interface{} `json:"metadata"`
}

func TimestampCheck(oldVal, newVal, existVal []byte) ([]byte, error) {
	oldV := MetaValue{}
	if len(oldVal) > 0 {
		err := json.Unmarshal(oldVal, &oldV)
		if err != nil {
			logrus.Warnf("old %s not valid, %s", oldVal, err)
			return nil, xerror.ErrCheckAndSetInvalid
		}
		if oldV.UpdatedAt < 0 {
			logrus.Warnf("old %s not valid", oldVal)
			return nil, xerror.ErrCheckAndSetInvalid
		}
	}

	existV := MetaValue{}
	if len(existVal) > 0 {
		err := json.Unmarshal(existVal, &existV)
		if err != nil {
			logrus.Warnf("exist %s not valid, %s", existVal, err)
			return nil, xerror.ErrCheckAndSetInvalid
		}
		if existV.UpdatedAt < 0 {
			logrus.Warnf("exist %s not valid", existVal)
			return nil, xerror.ErrCheckAndSetInvalid
		}
	}

	newV := MetaValue{}
	if len(newVal) > 0 {
		err := json.Unmarshal(newVal, &newV)
		if err != nil {
			logrus.Warnf("new %s not valid, %s", newVal, err)
			return nil, xerror.ErrCheckAndSetInvalid
		}

		if newV.UpdatedAt <= 0 {
			logrus.Warnf("new %s not valid", newVal)
			return nil, xerror.ErrCheckAndSetInvalid
		}
	}

	equal := reflect.DeepEqual(newV, existV)
	//logrus.Debugf("new %v, exist %v, equal %t", newV, existV, equal)
	if equal {
		return nil, xerror.ErrAlreadyExists
	}

	if oldV.UpdatedAt < existV.UpdatedAt {
		logrus.Warnf("old %s < exist %d", oldVal, existV.UpdatedAt)
		return nil, xerror.ErrCheckAndSetFailed
	}

	if len(newVal) == 0 {
		return nil, nil
	}

	if oldV.UpdatedAt > newV.UpdatedAt {
		logrus.Warnf("old %s > new %d", oldVal, newV.UpdatedAt)
		return nil, xerror.ErrCheckAndSetFailed
	}

	if existV.UpdatedAt > newV.UpdatedAt {
		logrus.Warnf("new %s < exist %d", newVal, existV.UpdatedAt)
		return nil, xerror.ErrCheckAndSetFailed
	}

	if newV.Deleted {
		return nil, nil
	}
	return newVal, nil
}
