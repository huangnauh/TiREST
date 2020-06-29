package model

type List struct {
	Start   []byte `json:"start"`
	End     []byte `json:"end"`
	Limit   int    `json:"limit"`
	KeyOnly bool   `json:"key-only"`
}
