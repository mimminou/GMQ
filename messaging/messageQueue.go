package messaging

import (
	"fmt"
	"net"
	"sync"
)

type Command struct {
	Method    string  `json:"method"`
	ChannelID int     `json:"chan"`
	Msg       Payload `json:"msg"`
}

type Client struct {
	ConnInfo   net.Conn
	ChannelID  int
	RoutingKey string // this is the name of the queue
	ClientType string // pub or sub
}

type Payload struct {
	ID         string `json:"id"`         // the id of the message, UUID
	RoutingKey string `json:"routingkey"` // the name of the queue
	Body       string `json:"body"`       //Body of the message
	BodyB64    []byte `json:"-"`          //Body of the message
}

type Queue struct {
	Name     string
	Messages chan Payload
	Subs     []Client
	subsLock sync.Mutex
}

type queueNamesMap struct {
	//this is the binding between the routing key and the messaging queue
	data map[string]*Queue
	mux  sync.Mutex
}

// keep track of our queues, because we have to distinguish them by name in order to prevent adding dup queues
var queueNames *queueNamesMap

// var queueClients *queueClientsMap

func init() {
	queueNames = &queueNamesMap{data: make(map[string]*Queue)} // The variable that keeps queues in memory
}

func MessageDispatcher(q *Queue) {
	fmt.Println("Starting message dispatcher")
	for {
		q.subsLock.Lock()
		q.subsLock.Unlock()
		fmt.Println("lenght of subs")
		fmt.Println(len(q.Subs))
		msg := q.Dequeue()

		if len(q.Subs) == 0 {
			continue
		}
		for _, sub := range q.Subs {
			fmt.Println(sub.ClientType)
			go SendMessage(sub.ConnInfo, msg)
		}
	}
}

// Sends a message to a client
func SendMessage(conn net.Conn, msg Payload) {
	// Sending to client
	fmt.Println("Sending message")
	if msg.ID == "" {
		fmt.Fprintf(conn, "%s\n", msg.Body)
		conn.Close()
		return
	}
	fmt.Fprintf(conn, "%s\n", msg.Body)
}

// Creates a new queue of messages with a specified capacity, returns a pointer to it, a new Queue is only created if a sub tries to subscribe to a queue that does not exist
func NewQueue(name string) (*Queue, error) {
	queueNames.mux.Lock()
	defer queueNames.mux.Unlock()

	fmt.Println("New queue request received")
	// if no queue exist, create one
	if len(queueNames.data) == 0 {
		queueNames.data[name] = &Queue{Name: name, Messages: make(chan Payload, 50), Subs: make([]Client, 0, 10)}
		go MessageDispatcher(queueNames.data[name])
		return queueNames.data[name], nil
	}
	// if queue exists, return it
	if value, ok := queueNames.data[name]; ok {
		return value, nil
	} else {
		queueNames.data[name] = &Queue{Name: name, Messages: make(chan Payload, 50), Subs: make([]Client, 0, 10)}
		go MessageDispatcher(queueNames.data[name])
		return queueNames.data[name], nil
	}
}

// Returns a reference to a queue and a bool based on the name passed, true if found, false if not found
func GetQueue(name string) (*Queue, bool) {
	queueNames.mux.Lock()
	defer queueNames.mux.Unlock()
	//print all existing queues
	for key := range queueNames.data {
		fmt.Println(key)
	}
	if value, ok := queueNames.data[name]; ok {
		return value, true
	}
	return nil, false
}

// Deletes a message Queue, "should" notify all subscribers that the queue is pending deletion, then deletes it
func DeleteQueue(name string) {
	// TODO : implement message notification, or rather sending messages to subscribers in order for notifications to work
	fmt.Println("Delete request received")
	queueNames.mux.Lock()
	defer queueNames.mux.Unlock()

	//TODO : notify subs
	delete(queueNames.data, name)
}

// Appends a message to a queue, will create the queue if it's non existent
func PublishMessage(m Payload) {
	fmt.Println("Publish request received")
	if q, exists := GetQueue(m.RoutingKey); exists {
		q.Enqueue(m)
		return
	}
	// if queue does not exist, create it
	q, err := NewQueue(m.RoutingKey)
	if err != nil {
		q.Enqueue(m)
	}
	return
}

// Adds a Subscriber to the clients map
func AddSub(c Client) { // maybe there should be error handling here
	fmt.Println("Sub request received")
	go func() {
		if currentQueue, exists := GetQueue(c.RoutingKey); exists {
			currentQueue.subsLock.Lock()
			defer currentQueue.subsLock.Unlock()
			currentQueue.Subs = append(currentQueue.Subs, c)
			return
		}
		SendMessage(c.ConnInfo, Payload{ID: "", RoutingKey: c.RoutingKey, Body: "Queue does not exist", BodyB64: nil})
	}()
}

// Private function, removes a client from a list of clients, since ordering does NOT matter, this replaces the client that will be removed by the last one and returns a slice of length-1 the original, thus avoiding shifting the slice
func removeClient(s []Client, index int) []Client {
	s[index] = s[len(s)-1]
	return s[:len(s)-1]
}

// removes Subscribers from the subscriptions map
func RemoveSub(c Client) {
	// need a go routine here or else it will block the thread that called this if the pool is full, the thread could be a network connection already so I might remove it
	fmt.Println("Unsub request received")
	go func() {
		// queueClients.mux.Lock()
		// defer queueClients.mux.Unlock()
		if currentQueue, exists := GetQueue(c.RoutingKey); exists {
			currentQueue.subsLock.Lock()
			defer currentQueue.subsLock.Unlock()
			for i, sub := range currentQueue.Subs {
				if sub == c {
					currentQueue.Subs = removeClient(currentQueue.Subs, i)
					break
				}
			}
			//Client not subbed, no op
		}
	}()
}

// Appends to the queue of messages, thread safe
func (q *Queue) Enqueue(m Payload) {
	// the goroutine here is needed, else it will block the thread that called this if the pool is full.
	go func() {
		q.Messages <- m
	}()
}

// Dequeues and returns a Message and a bool, true if a message was returned from the queue, false if it was empty
func (q *Queue) Dequeue() Payload {
	return <-q.Messages
}
