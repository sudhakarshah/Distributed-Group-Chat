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
		"bytes"
		"container/ring"
)

const (
		CONN_HOST = "0.0.0.0"
		CONN_TYPE = "tcp"
)

type connection struct {
	conn net.Conn

	// username (doesn't need to be unique)
	name string
}


// fixed size array containing all the IP addresses
var IpAddress = [...]string {"172.22.94.77", "172.22.156.69", "172.22.158.69",
							"172.22.94.78", "172.22.156.70", "172.22.158.70",
							"172.22.94.79", "172.22.156.71", "172.22.158.71",
							"172.22.94.80"}

// username
var name string
var wg sync.WaitGroup

// this is a string from "01", "02" .. "10" that is the vmNum of the host machine
var vmNum string

// -------- Reliability ------------------
var allMessages map[string]string = make(map[string]string)
var ownMessages map[string]string = make(map[string]string)

// -------- Causality --------------------
var outOfOrderMsgs map[string]string = make(map[string]string)
var VecTimestamp map[string]int


func main() {

	arguments := os.Args[1:]
	name = arguments[0]
	port := arguments[1]
	numberOfParticipants, _ := strconv.Atoi(arguments[2])
	VecTimestamp = make(map[string]int, numberOfParticipants)
	setVmNumber()
	var chans = make([]chan string,numberOfParticipants)
	for i := range chans {
		chans[i] = make(chan string)
	}


	// starting server to accept connection from all other nodes
	wg.Add(1)
	go server(port, numberOfParticipants, chans)

	// ring is used to iterate through all IP addresses
	// checking which ones have connected

	// ----------------------------RING--------------------------------------
	num_ips := len(IpAddress)
	IpRing := ring.New(num_ips)

	// initialize ring
	for itr := 0; itr < num_ips; itr++ {
		IpRing.Value = IpAddress[itr]
		IpRing = IpRing.Next()
	}

	// Keep iterate through Ip ring
	count := 0
	for {

		if (count >= numberOfParticipants) {
			fmt.Println("READY")
			break
		}
		ip := IpRing.Value
		address := ip.(string) + ":" + port
		conn, err := net.Dial("tcp", address)
		if err == nil {
			addr, _ := net.LookupAddr(ip.(string))
			remoteVmNum := addr[0][15:17]
			VecTimestamp[remoteVmNum] = 0
			go client(conn, chans[count])
			IpRing = IpRing.Prev()
			IpRing.Unlink(1)
			count++
		}
		IpRing = IpRing.Next()
	}
	// ----------------------------RING--------------------------------------


	// taking user input
	for {

		reader := bufio.NewReader(os.Stdin)
		text, _ := reader.ReadString('\n')

		// Increment timestamp of current process
		VecTimestamp[vmNum]++
		vecTimestampString := map_to_str(VecTimestamp)


		t := time.Now().String()
		t = strings.Join(strings.Fields(t),"")

		text = t + " " + vecTimestampString + " " + vmNum + " " + name + ": " + text
		ownMessages[t] = text

		// sending to all the channels
		for _, c := range chans {
			c <- text
			// time.Sleep(2 * time.Second)
		}
	}
}


// ################### SERVER & CLIENT ######################## //


func server(port string, connectionCount int, chans []chan string) {

	// Listen for incoming connections.
	l, err := net.Listen(CONN_TYPE, CONN_HOST + ":" + port)
	if err != nil {
			fmt.Println("Error listening:", err.Error())
			os.Exit(1)
	}

	// Close the listener when the application closes.
	defer l.Close()

	for i := 0; i < connectionCount; i++ {

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
	buf := make([]byte, 1024)


	for {

		// Read the incoming connection into the buffer.
		recLen, err := conn.conn.Read(buf)
		if err != nil {
			fmt.Println(conn.name + " has left")
			conn.conn.Close()
			return
		}
		text := string(buf[:int(recLen)])
		words := strings.Fields(text)

		// parse message
		sequenceNum := words[0]
		vecTsReceivedString := words[1]
		vecTsReceived := str_to_map(vecTsReceivedString)
		remoteVmNum := words[2]


		// atomically checking and resending to everyone if new message
		mutex.Lock()

		_, isOld := allMessages[sequenceNum]
		_, isMyOld := ownMessages[sequenceNum]


		if (!isMyOld && !isOld) {
			// this message has not been sent by me
			// this is the first time i am receiveing this message
			// so i need to figure out if i should keep this message or put it in a holdback queue

			// received for the first time hence send to all other servers to make it reliable
			

			deliverNow := verifyCausalOrdering(vecTsReceived, remoteVmNum)

			if deliverNow {

				for _, c := range chans {
					c <- text
				}

				updateTimestamp(vecTsReceived, remoteVmNum)

				// print the message to the screen
				msg := strings.Join(words[3:], " ")
				fmt.Printf("%v\n", msg);

				// add to running list of all messages received
				allMessages[sequenceNum] = text

				// check if any buffered messages can now be delivered
				// since the timestamps have been updated
				deliverBufferedMsgs(chans)

			} else {
				// fmt.Println("Violates causal ordering")
				// add this message that violates causality to buffer queue
				outOfOrderMsgs[words[0]] = text
			}
		}

		if (!isOld && isMyOld) {
			// this is a message i sent to ....myself received this... sent it to myself
			// so print it and add it to list of all messages received

			msg := strings.Join(words[3:], " ")
			fmt.Printf("%v\n", msg);

			allMessages[words[0]] = text
		}

		mutex.Unlock()
	}

	// Close the connection when you're done with it.
	conn.conn.Close()
}



func client(conn net.Conn, c chan string) {

	fmt.Fprintf(conn, name)
	for {
		text := <- c
		// read in input from stdin
		// msg := message{name, text}
		// send to socket
		fmt.Fprintf(conn, text)
	}

}



// ################### CAUSALITY ENSURING HELPER FUNCTIONS ######################## //



func deliverBufferedMsgs(chans []chan string) {
	// iterate over them and run them through the verifyCausalOrdering func
	// then execute the (!isOld && keep) condition to print these messages
	// that have just been delivered

	deliveredKeys := make([]string, 0)

	for key, text := range outOfOrderMsgs {

		words := strings.Fields(text)

		// parse message
		sequenceNum := words[0]
		vecTsReceivedString := words[1]
		vecTsReceived := str_to_map(vecTsReceivedString)
		remoteVmNum := words[2]

		deliverNow := verifyCausalOrdering(vecTsReceived, remoteVmNum)

		if deliverNow {

			for _, c := range chans {
				c <- text
			}

			updateTimestamp(vecTsReceived, remoteVmNum)

			// print the message to the screen
			msg := strings.Join(words[3:], " ")
			fmt.Printf("%v\n", msg);

			// add to running list of all messages received
			allMessages[sequenceNum] = text

			deliveredKeys = append(deliveredKeys, key)
		}
	}

	if len(deliveredKeys) > 0 {

		// remove the msgs that got delivered and repeat the process
		for _, key := range deliveredKeys {
			delete(outOfOrderMsgs, key)
		}

		deliverBufferedMsgs(chans)
	}
}


// verifies if the message we received was causally ordered according to
// conditions specified in this video: https://www.youtube.com/watch?v=lugR1CIIU4w
func verifyCausalOrdering(vecTsReceived map[string]int, remoteVmNum string) bool {

	if vecTsReceived[remoteVmNum] != (VecTimestamp[remoteVmNum] + 1) {
		return false
	}

	for key, val := range vecTsReceived {
		if ((key != remoteVmNum) && (val > VecTimestamp[key])) {
			return false
		}
	}

	return true
}


// update timestamp based on new message received
func updateTimestamp(vecTsReceived map[string]int, remoteVmNum string) {
	VecTimestamp[remoteVmNum] = vecTsReceived[remoteVmNum]
}



// ################### HELPER FUNCTIONS ######################## //


// sets the global variable vmNum
func setVmNumber() {

	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
		os.Exit(1)
	}

	// from hostname we can extract VM number
	// this is a string from "01", "02" ... "10"
	vmNum = hostname[15:17]

}


// converts a map to string
func map_to_str(m map[string]int) string {
	b := new(bytes.Buffer)
	for key, val := range m {
		fmt.Fprintf(b, "%s=%d;", key, val)
	}
	return b.String()
}


// converts a string to a map
func str_to_map(s string) map[string]int {
	elems := strings.Split(s, ";")
	m := make(map[string]int, len(elems)-1)
	for _, elem := range elems[:len(elems)-1] {
		key_val := strings.Split(elem, "=")
		key := key_val[0]
		val, _ := strconv.Atoi(key_val[1])
		m[key] = val
	}
	return m
}