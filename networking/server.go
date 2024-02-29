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

	//run the message dispatcher
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
			fmt.Fprintf(conn, "Error decoding JSON: %s\n", err)
			fmt.Printf("JSON DECODE ERR: %s\n", err)
			return
		}
		// we are in the clear, message is guarenteed to be JSON
		handleMethod(msg, conn)
	}
}

func handleMethod(msg messaging.Command, conn net.Conn) {
	switch msg.Method {
	case "pub":
		payload, err := extractPayload(msg)
		if err != nil {
			fmt.Fprintf(conn, "Error decoding JSON: %s\n", err)
			return
		}
		messaging.PublishMessage(messaging.Payload{
			ID:         payload.ID,
			RoutingKey: payload.RoutingKey,
			Body:       payload.Body,
			BodyB64:    nil,
		})
		return

	// Need to ignore payload message for all cases under this one, to save a bit of processing power when parsing JSON
	case "sub":
		payload, err := extractPayload(msg)
		if err != nil {
			fmt.Fprintf(conn, "Error decoding JSON: %s\n", err)
			return
		}
		messaging.AddSub(messaging.Client{
			ConnInfo:   conn,
			ChannelID:  msg.ChannelID,
			RoutingKey: payload.RoutingKey,
			ClientType: msg.Method,
		})

	case "unsub":
		payload, err := extractPayload(msg)
		if err != nil {
			fmt.Fprintf(conn, "Error decoding JSON: %s\n", err)
			return
		}
		messaging.RemoveSub(messaging.Client{
			ConnInfo:   conn,
			ChannelID:  msg.ChannelID,
			RoutingKey: payload.RoutingKey,
			ClientType: msg.Method,
		})

	case "new":
		payload, err := extractPayload(msg)
		if err != nil {
			fmt.Fprintf(conn, "Error decoding JSON: %s\n", err)
			return
		}
		messaging.NewQueue(payload.RoutingKey)
		return

	case "del":
		payload, err := extractPayload(msg)
		if err != nil {
			fmt.Fprintf(conn, "Error decoding JSON: %s\n", err)
			return
		}
		messaging.DeleteQueue(payload.RoutingKey)

	case "queues":
		_, err := extractPayload(msg)
		if err != nil {
			fmt.Fprintf(conn, "Error decoding JSON: %s\n", err)
			return
		}
		messaging.SendAllQueues(conn)
		return
	}
}

func extractPayload(msg messaging.Command) (messaging.Payload, error) {
	return msg.Msg, nil
}
