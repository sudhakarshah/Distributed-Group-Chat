package main

import (
		"fmt"
		"net"
		"os"
		"bufio"
		"strconv"
		"sync"

)

const (
		CONN_HOST = "0.0.0.0"
		// CONN_PORT = "3333"
		CONN_TYPE = "tcp"
)

type message struct {
	name string
	data string
}


// fixed size array containing all the IP addresses
var IpAddress = [...]string {"172.22.94.77", "172.22.156.69", "172.22.158.69"}
// var IpAddress = [...]string {"localhost"}
var name string
var wg sync.WaitGroup
func main() {
		// argsWithProg := os.Args
		arguments := os.Args[1:]
		name = arguments[0]
		port := arguments[1]
		fmt.Println(name+ port)
		numberOfParticipants, err := strconv.Atoi(arguments[2])
		if (err != nil) {

		}
		// var chans [5]chan string
		var chans = make([]chan string,numberOfParticipants)
		for i := range chans {
			chans[i] = make(chan string)
		}
		// starting server to accept connection from all other nodes
		wg.Add(1)
		go server(port, numberOfParticipants)
		ownIp := get_own_ip()
		fmt.Println(ownIp  + " this is my own ip")
		count := 0
		for index, ip := range IpAddress {
			if (index == numberOfParticipants) {
				break
			}
			// if (ownIp == ip) {
			// 	continue
			// }
			wg.Add(1)
			fmt.Println("creating routine for "+ ip)
			go client(ip + ":" + port, chans[count])
			count++

		}

		wg.Wait()

		// taking user input
		for {
			reader := bufio.NewReader(os.Stdin)
			fmt.Println("Text to send:")
			text, _ := reader.ReadString('\n')
			// sending to all the channels
			for _, c := range chans {
				c <- text
			}
			// msg := message{name, text}
		}


}

func server(port string, connectionCount int) {
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
				conn, err := l.Accept()
				if err != nil {
						fmt.Println("Error accepting: ", err.Error())
						os.Exit(1)
				}
				// Handle connections in a new goroutine.
				go handleRequest(conn)
		}
		wg.Done()
}

// Handles incoming requests.
func handleRequest(conn net.Conn) {
	// Make a buffer to hold incoming data.
	fmt.Println("New connection added to server")
	buf := make([]byte, 1024)
	// Read the incoming connection into the buffer.
	for {
		recLen, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading:", err.Error())
		}
		fmt.Printf("received %v\n", string(buf[:int(recLen)]))
		// Send a response back to person contacting us.
		// conn.Write([]byte("Message received."))
	}

	// Close the connection when you're done with it.
	conn.Close()
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
	wg.Done()
	for {
		text := <- c
		// read in input from stdin
		// reader := bufio.NewReader(os.Stdin)
		// fmt.Println("Text to send to "+ address)
		// text, _ := reader.ReadString('\n')
		msg := message{name, text}
		// send to socket
		fmt.Fprintf(conn, msg.name +": " + msg.data + "\n")
		// listen for reply
		// message, _ := bufio.NewReader(conn).ReadString('\n')
		// fmt.Print("Message from server: "+message)
	}

}


// function returns own IP
func get_own_ip()string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops: " + err.Error() + "\n")
		os.Exit(1)
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
