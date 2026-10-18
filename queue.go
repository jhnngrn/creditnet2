package creditnet

import "time"

// QueueEntry is a single queued method with the time it was queued.
type QueueEntry struct {
	Method string `json:"method"`
	Time   int64  `json:"time,omitempty"`
}

// Queue is an ordered list of pending methods to send to a peer.
type Queue struct {
	Entries []QueueEntry `json:"entries,omitempty"`
}

func (q *Queue) Enqueue(method string) {
	q.Entries = append(q.Entries, QueueEntry{method, time.Now().Unix()})
}

func (q *Queue) Dequeue() {
	if len(q.Entries) > 0 {
		q.Entries = q.Entries[1:]
	}
}

func (q *Queue) Head() string {
	if len(q.Entries) > 0 {
		return q.Entries[0].Method
	}
	return ""
}

func (q *Queue) HeadTime() int64 {
	if len(q.Entries) > 0 {
		return q.Entries[0].Time
	}
	return 0
}

func (q *Queue) Has(method string) bool {
	for _, e := range q.Entries {
		if e.Method == method {
			return true
		}
	}
	return false
}

func (q *Queue) Len() int {
	return len(q.Entries)
}

func (q *Queue) Empty() bool {
	return len(q.Entries) == 0
}
