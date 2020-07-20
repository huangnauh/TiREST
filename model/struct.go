package model

type List struct {
	Start   string `header:"X-Start" json:"start"`
	End     string `header:"X-End" json:"end"`
	Limit   int    `header:"X-Limit" json:"limit"`
	Reverse bool   `header:"X-Reverse" json:"reverse"`
	KeyOnly bool   `header:"X-Key-Only" json:"key-only"`
	Unsafe  bool   `header:"X-Unsafe" json:"unsafe"`
	Raw     bool   `header:"X-Raw" json:"raw"`
}

type Meta struct {
	Force     bool   `header:"X-Force" json:"force"`
	Secondary string `header:"X-Secondary" json:"secondary"`
}
