package networking

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/mimminou/GMQ/messaging"
	"io"
	"net"
)

//TCP server that supports channels, but not msg ordering (because sequencing needs to be done at client level, I'm not willing to write libs for most of langs out there)

func StartTCPServer(port int) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Server Listening on port ", port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close() //close the connection when we're done
	// Read data from the connection
	buffer := make([]byte, 1024)
	// Loop to continue reading for more messages
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Error reading from connection: %s\n", err)
			}
			break
		}
		decoder := json.NewDecoder(bytes.NewReader(buffer[:n]))
		var msg messaging.Command
		err = decoder.Decode(&msg)
		if err != nil {
			fmt.Printf("Error decoding JSON: %s\n", err)
			return
		}
		// we are in the clear, message is guarenteed to be Json
		handleMethod(msg, conn)
		if msg.Method == "pub" {
			break
		}

	}
}

func handleMethod(msg messaging.Command, conn net.Conn) {
	switch msg.Method {
	case "pub":
		messaging.PublishMessage(messaging.Payload{
			ID:         msg.Payload,
			RoutingKey: msg.RoutingKey,
			Body:       msg.Payload,
			BodyB64:    nil,
		})
		break
	case "sub":
		//TODO : handle subbing, ano other functions like Delete queue, unsub and new Queue
	}

}
