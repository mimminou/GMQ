package messaging

import (
	"net"
	"sync"
)

type Command struct {
	Method     string `json:"method"`
	RoutingKey string `json:"queue"`
	ChannelID  int    `json:"channelID"`
	Payload    string `json:"msg"`
}

type Client struct {
	connInfo   net.Conn
	channelID  int
	routingKey string // this is the name of the queue
	clientType string // pub or sub
}

type Payload struct {
	ID         string `json:"id"`         // the id of the message, UUID
	RoutingKey string `json:"routingKey"` // the name of the queue
	Body       string `json:"body"`       //Body of the message
	BodyB64    []byte `json:"-"`          //Body of the message
}

type Queue struct {
	Name     string
	Messages chan Payload
}

type queueClientsMap struct {
	data map[*Queue][]Client
	mux  sync.Mutex
}

type queueNamesMap struct {
	//this is the binding between the routing key and the messaging queue
	data map[string]*Queue
	mux  sync.Mutex
}

// keep track of our queues, because we have to distinguish them by name and prevent adding dup queues
var queueNames *queueNamesMap // a client can't just remove queues, in this system, i'll build an observer that watches the queue, if it's empty and nothing is subscriber to it, delete that queue
var queueClients *queueClientsMap

func init() {
	queueNames = &queueNamesMap{data: make(map[string]*Queue)}       // The variable that keeps queues in memory
	queueClients = &queueClientsMap{data: make(map[*Queue][]Client)} // The variable that stores which clients are subbed to which Queues
}

// Creates a new queue of messages with a specified capacity, returns a pointer to it, a new Queue is only created if a sub tries to subscribe to a queue that does not exist
func NewQueue(name string) (*Queue, error) {
	queueNames.mux.Lock()
	defer queueNames.mux.Unlock()

	// if no queue exist, create one
	if len(queueNames.data) == 0 {
		queueNames.data[name] = &Queue{Name: name, Messages: make(chan Payload, 20)}
		return queueNames.data[name], nil
	}
	// if queue exists, return it
	if value, ok := queueNames.data[name]; ok {
		return value, nil
	} else {
		queueNames.data[name] = &Queue{Name: name, Messages: make(chan Payload, 20)}
		return queueNames.data[name], nil
	}
}

// Returns a reference to a queue and a bool based on the name passed, true if found, false if not found
func GetQueue(name string) (*Queue, bool) {
	queueNames.mux.Lock()
	defer queueNames.mux.Unlock()
	if value, ok := queueNames.data[name]; ok {
		return value, true
	}
	return nil, false
}

// Deletes a message Queue, will notify all subscribers that the queue is pending deletion, then deletes it
func (q *queueNamesMap) DeleteQueue(name string) {
	//There is no need to remove subs from that queue, only the queue itself as there could be a scenario where the queue is recreated with the same keybind routing
	q.mux.Lock()
	defer q.mux.Unlock()
	delete(q.data, name)
}

// Appends a message to a queue, will create the queue if it's non existent
func PublishMessage(m Payload) {
	if q, exists := GetQueue(m.RoutingKey); exists {
		q.Enqueue(m)
		return
	}
	q, err := NewQueue(m.RoutingKey)
	if err != nil {
		q.Enqueue(m)
	}
	return
}

// Adds a Subscriber to the clients map
func (q *queueClientsMap) AddSub(c Client) <-chan bool { // maybe returning bool is not the best solution, should return err instead
	ch := make(chan bool)
	go func() {
		q.mux.Lock()
		defer q.mux.Unlock()
		//check if the queue exists in the first place
		if requestedQueue, exists := GetQueue(c.routingKey); exists {
			//if queue exists, check if the client is already there or not
			for _, client := range q.data[requestedQueue] {
				if client == c && c.clientType == "sub" { // if client already subbed
					ch <- false
					return
				}
			}
			//Queue exists, but the client is not subbed, happy path, add the client
			q.data[requestedQueue] = append(q.data[requestedQueue], c)
			ch <- true
			return
		}
		//Queue does NOT exist, return an error
		ch <- false
		return
	}()
	return ch
}

// Private function, removes a client from a list of clients, since ordering does NOT matter, this replaces the client that will be removed by the last one and returns a slice of length-1 the original, thus avoiding shifting the slice
func removeClient(s []Client, index int) []Client {
	s[index] = s[len(s)-1]
	return s[:len(s)-1]
}

// removes Subscribers from the subscriptions map
func (q *queueClientsMap) RemoveSub(c Client) {
	// need a go routine here or else it will block the thread that called this if the pool is full, the thread could be a network connection already so I might remove it
	go func() {
		q.mux.Lock()
		defer q.mux.Unlock()
		if currentQueue, exists := GetQueue(c.routingKey); exists {
			for i, sub := range q.data[currentQueue] {
				if sub == c {
					q.data[currentQueue] = removeClient(q.data[currentQueue], i)
					break
				}
			}
			//Client not subbed, no op (yes i'm mimicking rabbitMQ, I'm learning here folks)
		}
	}()
}

// Appends to the queue of messages, thread safe
func (q *Queue) Enqueue(m Payload) {
	// need a go routine here or else it will block the thread that called this if the pool is full, the thread could be a network connection already so I might remove it
	go func() {
		q.Messages <- m
	}()
}

// Dequeues and returns a Message and a bool, true if a message was returned from the queue, false if it was empty
func (q *Queue) Dequeue() (Payload, bool) {
	select {
	case m := <-q.Messages:
		return m, true
	default:
		return Payload{}, false
	}
}
