package messaging

import (
	"fmt"
	"net"
	"sync"
	"time"
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

// keep track of our queues, because we have to distinguish them by name in order to prevent adding dup queues
var queueNames *queueNamesMap
var queueClients *queueClientsMap
var queueReady chan *Queue

func init() {
	queueNames = &queueNamesMap{data: make(map[string]*Queue)}       // The variable that keeps queues in memory
	queueClients = &queueClientsMap{data: make(map[*Queue][]Client)} // The variable that stores which clients are subbed to which Queues
	queueReady = make(chan *Queue)
}

// goroutine to iterate through queueClients, and sends a message to each Client
func MessageDispatcher() {
	//This is continously polling even when there are no messages, needs some optimizations, might need to create a message dispatcher for each queue
	fmt.Println("Starting message dispatcher")
	for {
		queueClients.mux.Lock()
		for queue, clients := range queueClients.data {
			if msg, hasMessage := queue.Dequeue(); hasMessage {
				for _, client := range clients {
					go SendMessage(client.ConnInfo, msg)
				}
			}
		}
		queueClients.mux.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// Sends a message to a client
func SendMessage(conn net.Conn, msg Payload) {
	// Sending to client
	fmt.Println("Sending message")
	fmt.Fprintf(conn, "%s\n", msg.Body)
}

// Creates a new queue of messages with a specified capacity, returns a pointer to it, a new Queue is only created if a sub tries to subscribe to a queue that does not exist
func NewQueue(name string) (*Queue, error) {
	fmt.Println("New queue request received")
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
// TODO : implement message notification, or rather sending messages to subscribers in order for notifications to work
func DeleteQueue(name string) {
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
	q, err := NewQueue(m.RoutingKey)
	if err != nil {
		q.Enqueue(m)
	}

	return
}

// Adds a Subscriber to the clients map
func AddSub(c Client) { // maybe returning bool is not the best solution, should return err instead
	fmt.Println("Sub request received")
	go func() {
		queueClients.mux.Lock()
		defer queueClients.mux.Unlock()
		//check if the queue exists in the first place
		if requestedQueue, exists := GetQueue(c.RoutingKey); exists {
			//if queue exists, check if the client is already there or not
			for _, client := range queueClients.data[requestedQueue] {
				if client == c && c.ClientType == "sub" { // if client already subbed
					return
				}
			}
			//Queue exists, but the client is not subbed, happy path, add the client
			queueClients.data[requestedQueue] = append(queueClients.data[requestedQueue], c)
			return
		}
		//Queue does NOT exist, no op (for now, need to send an err in this case)
		SendMessage(c.ConnInfo, Payload{ID: "0", RoutingKey: c.RoutingKey, Body: "Queue does not exist", BodyB64: nil})
		return
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
		queueClients.mux.Lock()
		defer queueClients.mux.Unlock()
		if currentQueue, exists := GetQueue(c.RoutingKey); exists {
			for i, sub := range queueClients.data[currentQueue] {
				if sub == c {
					queueClients.data[currentQueue] = removeClient(queueClients.data[currentQueue], i)
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
