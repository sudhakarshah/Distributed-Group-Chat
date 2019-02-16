package main

import (
		"fmt"
		"net"
		"os"
		"bufio"
		"strconv"
		"sync"
		"time"
		"strings"
)

const (
		CONN_HOST = "0.0.0.0"
		CONN_TYPE = "tcp"
)

type message struct {
	name string
	// do describe the
	// type string
	data string
}

type connection struct {
	conn net.Conn
	// do describe the
	// type string
	name string
}
// fixed size array containing all the IP addresses
var IpAddress = [...]string {"172.22.94.77", "172.22.156.69", "172.22.158.69", "172.22.95.255", "172.22.157.255", "172.22.159.255"}
// var IpAddress = [...]string {"localhost"}
var name string
var wg sync.WaitGroup
var allMessages map[string]string = make(map[string]string)
var ownMessages map[string]string = make(map[string]string)

func main() {
		arguments := os.Args[1:]
		name = arguments[0]
		port := arguments[1]
		numberOfParticipants, _ := strconv.Atoi(arguments[2])

		var chans = make([]chan string,numberOfParticipants)
		for i := range chans {
			chans[i] = make(chan string)
		}
		// starting server to accept connection from all other nodes
		wg.Add(1)
		go server(port, numberOfParticipants, chans)

		count := 0
		for _, ip := range IpAddress {
			if (count == numberOfParticipants) {
				break
			}
			wg.Add(1)
			fmt.Println("creating go routine for "+ ip)
			go client(ip + ":" + port, chans[count])
			//
			count++
		}

		// waiting for all clients to connect and server to connect to all other nodes
		wg.Wait()

		// taking user input
		for {
			reader := bufio.NewReader(os.Stdin)
			fmt.Println("Text to send:")
			text, _ := reader.ReadString('\n')
			// sending to all the channels
			t := time.Now().String()
			t = strings.Join(strings.Fields(t),"")
			fmt.Println(t)
			text = t + " " + name + " " + text
			ownMessages[t] = text
			for _, c := range chans {
				c <- text
				// time.Sleep(2 * time.Second)
			}
		}


}

func server(port string, connectionCount int, chans []chan string) {
		fmt.Println("server\n")
		// Listen for incoming connections.
		l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+port)
		if err != nil {
				fmt.Println("Error listening:", err.Error())
				os.Exit(1)
		}
		// Close the listener when the application closes.
		defer l.Close()
		fmt.Println("Listening on " + CONN_HOST + ":" + port)
		for i :=0; i<connectionCount; i++ {
				fmt.Println("Entered  for loop to listen")
				// Listen for an incoming connection.
				conn := connection{}
				var err error
				conn.conn, err = l.Accept()
				if err != nil {
						fmt.Println("Error accepting: ", err.Error())
						os.Exit(1)
				}

				// receiving name
				buf := make([]byte, 1024)
				recLen, er := conn.conn.Read(buf)
				if er != nil {
					fmt.Println("name reading error", err.Error())
					conn.conn.Close()
					return
				}
				conn.name = string(buf[:int(recLen)])
				// Handle connections in a new goroutine.
				go handleRequest(conn, chans)
		}
		wg.Done()
}

// Handles incoming requests.

var mutex = &sync.Mutex{}
func handleRequest(conn connection, chans []chan string) {
	// Make a buffer to hold incoming data.
	fmt.Println("New connection added to server with: "+ conn.name)
	buf := make([]byte, 1024)
	// Read the incoming connection into the buffer.

	for {

		recLen, err := conn.conn.Read(buf)
		if err != nil {
			fmt.Println(conn.name + " has left")
			conn.conn.Close()
			return
		}
		text := string(buf[:int(recLen)])
		words := strings.Fields(text)


		// atomically checking and resending to everyone if new message
		mutex.Lock()
		_, isOld := allMessages[words[0]]
		_, isMyOld := ownMessages[words[0]]
		// received for the first time hence send to all other servers
		if (!isOld && !isMyOld) {
			for _, c := range chans {
				c <- text
			}

		}
		if(!isOld) {
			fmt.Printf("received %v\n", text)
			// string(words[1] + " " + words[2])
			allMessages[words[0]] = text
		}
		mutex.Unlock()


		// Send a response back to person contacting us.
		// conn.Write([]byte("Message received."))
	}

	// Close the connection when you're done with it.
	conn.conn.Close()
}


func client(address string, c chan string) {

	// connect to this socket
	fmt.Println(address+" routine created")
	conn, err := net.Dial("tcp", address)

	// loop till client can't connect to server
	for err != nil {
		// fmt.Println("retryig connecting to server")
		conn, err = net.Dial("tcp", address)
		// can introduce some sleep here
	}
	fmt.Fprintf(conn, name)
	wg.Done()
	for {
		text := <- c
		// words := strings.Fields(text)
		// read in input from stdin
		// msg := message{name, text}
		// send to socket
		fmt.Fprintf(conn, text)
	}

}
