package model

type List struct {
	Start   []byte
	End     []byte
	Limit   int
	KeyOnly bool
}
