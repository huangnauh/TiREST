package model

type List struct {
	Start   string `header:"X-Start" json:"start"`
	End     string `header:"X-End" json:"end"`
	Limit   int    `header:"X-Limit" json:"limit"`
	KeyOnly bool   `header:"X-Key-Only" json:"key-only"`
}
