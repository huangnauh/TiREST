package server

import (
	"github.com/huangnauh/tirest/config"
	"github.com/huangnauh/tirest/store"
)

func DefaultGetOption() store.GetOption {
	return store.GetOption{}
}

func DefaultListOption() store.ListOption {
	return store.ListOption{
		Item: ItemFunc,
	}
}

func defaultCheckOption() store.CheckOption {
	return store.CheckOption{
		Check: TimestampCheck,
	}
}

func timestampCheckOption() store.CheckOption {
	return store.CheckOption{
		Check: TimestampCheck,
	}
}

func exactCheckOption() store.CheckOption {
	return store.CheckOption{
		Check: ExactCheck,
	}
}

func nopCheckOption() store.CheckOption {
	return store.CheckOption{
		Check: NopCheck,
	}
}

func GetCheckOption(co config.CheckOption) store.CheckOption {
	switch co {
	case config.TimestampCheck:
		return timestampCheckOption()
	case config.ExactCheck:
		return exactCheckOption()
	case config.NopCheck:
		return nopCheckOption()
	default:
		return defaultCheckOption()
	}
}
