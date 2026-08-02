package event

import "time"

type ClickEvent struct {
	ShortCode string    `json:"short_code"`
	Timestamp time.Time `json:"timestamp"`
	IP        string    `json:"ip"`
}
