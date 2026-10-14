package creditnet

// Queue helpers for module method queues.

func Enqueue(q *[]string, method string) { *q = append(*q, method) }
func Dequeue(q *[]string)                { if len(*q) > 0 { *q = (*q)[1:] } }
func QueueHead(q []string) string        { if len(q) > 0 { return q[0] }; return "" }
