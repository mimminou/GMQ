package messaging

import (
	"fmt"
	"net"
	"strings"
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
		msg := q.Dequeue()

		if len(q.Subs) == 0 {
			continue
		}
		for _, sub := range q.Subs {
			go SendMessage(sub.ConnInfo, msg)
		}
	}
}

// Sends a message to a client
func SendMessage(conn net.Conn, msg Payload) {
	// Sending to client
	fmt.Println("Sending message")
	fmt.Fprintf(conn, "%s\n", msg.Body)
}

// Creates a new queue of messages with a specified capacity, returns a pointer to it or to the queue of the same name if it exists already. A new queue is only created if a publisher tries to publish to a queue that does not exist
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
		// queue does not exist, create it
		queueNames.data[name] = &Queue{Name: name, Messages: make(chan Payload, 50), Subs: make([]Client, 0, 10)}
		go MessageDispatcher(queueNames.data[name])
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

// gets all existing queues and returns a slice of their names
func getAllQueues() []string {
	//print all existing queues
	queueNames.mux.Lock()
	defer queueNames.mux.Unlock()
	queueList := make([]string, 0)
	for key := range queueNames.data {
		queueList = append(queueList, key)
	}
	return queueList
}

// Public function, sends all existing queues to the client
func SendAllQueues(conn net.Conn) {
	//send all existing queues to the client
	queueList := getAllQueues()
	queueListString := strings.Join(queueList, ", ")
	SendMessage(conn, Payload{Body: fmt.Sprintf("%v", queueListString)})
}

// Deletes a message Queue, "should" notify all subscribers that the queue is pending deletion, then deletes it
func DeleteQueue(name string) {
	fmt.Println("Delete request received")
	queueNames.mux.Lock()
	defer queueNames.mux.Unlock()

	//TODO : notify subs about queue deletion before this
	delete(queueNames.data, name)
}

// Appends a message to a queue, will create the queue if it's non existent
func PublishMessage(m Payload) {
	fmt.Println("Publish request received")
	// newQueue will check if Q exists or not, will return either a ref if it exists, or creates a new one and returns a ref to it
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
		SendMessage(c.ConnInfo, Payload{ID: "", RoutingKey: c.RoutingKey, Body: "Queue does not exist", BodyB64: nil}) // ID of empty string means queue does not exist
	}()
}

// Private function, removes a client from a list of clients, since ordering does NOT matter, this replaces the client that will be removed by the last one and returns a slice of length-1 the original, thus avoiding shifting the slice
func removeClient(s []Client, index int) []Client {
	// if only one client, return empty slice with len 0 and capacity 10
	if len(s) == 1 {
		return make([]Client, 0, 10)
	}
	s[index] = s[len(s)-1]
	return s[:len(s)-1]
}

// removes Subscribers from the subscriptions map
func RemoveSub(c Client) {
	// need a go routine here or else it will block the thread that called this if the pool is full
	fmt.Println("Unsub request received")
	go func() {
		if currentQueue, exists := GetQueue(c.RoutingKey); exists {
			currentQueue.subsLock.Lock()
			defer currentQueue.subsLock.Unlock()
			//check if client is subbed, iterate thourgh clients list in this particular queue
			for i, sub := range currentQueue.Subs {
				if c == sub {
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

// Dequeues and returns a Message, will block until a message is available
func (q *Queue) Dequeue() Payload {
	return <-q.Messages
}
