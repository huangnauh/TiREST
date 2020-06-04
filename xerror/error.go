package xerror

import "errors"

var ErrNotExists = errors.New("not exists")
var ErrGetKVFailed = errors.New("get kv failed")
var ErrSetKVFailed = errors.New("set kv failed")
var ErrListKVFailed = errors.New("list kv failed")
var ErrCommitKVFailed = errors.New("commit kv failed")
var ErrGetTimestampFailed = errors.New("get timestamp failed")
var ErrCheckAndSetFailed = errors.New("check and set failed")
