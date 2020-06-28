package xerror

import "errors"

var ErrExists = errors.New("exists")
var ErrNotExists = errors.New("not exists")
var ErrNotRegister = errors.New("not register")
var ErrGetKVFailed = errors.New("get kv failed")
var ErrSetKVFailed = errors.New("set kv failed")
var ErrListKVFailed = errors.New("list kv failed")
var ErrCommitKVFailed = errors.New("commit kv failed")
var ErrDatabaseNotExists = errors.New("database not exists")
var ErrCheckAndSetFailed = errors.New("check and set failed")
var ErrGetTimestampFailed = errors.New("get timestamp failed")
var ErrConnectorNotExists = errors.New("connector not exists")
