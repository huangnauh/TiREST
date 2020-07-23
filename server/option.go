package server

import "gitlab.s.upyun.com/platform/tikv-proxy/store"

func DefaultGetOption() store.GetOption {
	return store.GetOption{}
}

func DefaultListOption() store.ListOption {
	return store.ListOption{
		Item: ItemFunc,
	}
}

func DefaultCheckOption() store.CheckOption {
	return store.CheckOption{
		Check: TimestampCheck,
	}
}
