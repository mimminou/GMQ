package messaging

type Message struct {
	ID      string
	Content []byte
}

type Queue struct {
	Messages chan Message
}

func NewQueue(capacity int) *Queue {
	return &Queue{Messages: make(chan Message, capacity)}
}

func (q *Queue) Enqueue(m Message) {
	go func() {
		q.Messages <- m
	}()
}

func (q *Queue) Dequeue() Message {
	return <-q.Messages
}
