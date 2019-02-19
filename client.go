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
var IpAddress = [...]string {"172.22.94.77", "172.22.156.69", "172.22.158.69", 
							"172.22.94.78", "172.22.156.70", "172.22.158.70",
							"172.22.94.79", "172.22.156.71", "172.22.158.71",
							"172.22.94.80"}
// var IpAddress = [...]string {"localhost"}
var name string
var wg sync.WaitGroup
var allMessages map[string]string = make(map[string]string)
var ownMessages map[string]string = make(map[string]string)
var outOfOrderMsgs map[string]string = make(map[string]string)
// initialize timestamp to all zeros
var VecTimestamp map[string]int
var vm_num string
var start_flag int

func main() {
		start_flag = 0
		arguments := os.Args[1:]
		name = arguments[0]
		port := arguments[1]
		numberOfParticipants, _ := strconv.Atoi(arguments[2])
	
		// we need hostname for figuring out what our index 
		// in the vector timestamp is
		hostname, err := os.Hostname()
		if err != nil {
			panic(err)
			os.Exit(1)
		}

		// from hostname we can extract VM number
		vm_num = hostname[15:17]

		VecTimestamp = make(map[string]int, numberOfParticipants)

		var chans = make([]chan string,numberOfParticipants)
		for i := range chans {
			chans[i] = make(chan string)
		}
		// starting server to accept connection from all other nodes
		wg.Add(1)
		go server(port, numberOfParticipants, chans)

		
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
				VecTimestamp[addr[0][15:17]] = 0
				go client(conn, chans[count])
				IpRing = IpRing.Prev()
				IpRing.Unlink(1)
				count++
			} 
			IpRing = IpRing.Next()
		}
		
		
		// taking user input
		for {
			reader := bufio.NewReader(os.Stdin)
			//fmt.Println("Text to send:")
			text, _ := reader.ReadString('\n')
			// sending to all the channels
			t := time.Now().String()
			t = strings.Join(strings.Fields(t),"")
			// Increment timestamp of current process 
			// TODO: Is this the best place to increment timestamp for send?
			VecTimestamp[vm_num]++
			timestamp_str := map_to_str(VecTimestamp)

			text = t + " " + timestamp_str + " " + vm_num + " " + name + ": " + text
			ownMessages[t] = text
			for _, c := range chans {
				c <- text
				// time.Sleep(2 * time.Second)
			}
		}


}

func map_to_str(m map[string]int) string {
	b := new(bytes.Buffer)
	for key, val := range m {
		fmt.Fprintf(b, "%s=%d;", key, val)
	}
	return b.String()
}

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

func server(port string, connectionCount int, chans []chan string) {
		//fmt.Println("server\n")
		// Listen for incoming connections.
		l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+port)
		if err != nil {
				fmt.Println("Error listening:", err.Error())
				os.Exit(1)
		}
		// Close the listener when the application closes.
		defer l.Close()
		//fmt.Println("Listening on " + CONN_HOST + ":" + port)
		for i :=0; i<connectionCount; i++ {

				//fmt.Println("Entered  for loop to listen")
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
				if (vm_num == "03") && (start_flag == 0) {
					start_flag = 1
					time.Sleep(5*time.Second)
					continue
				}
				go handleRequest(conn, chans)
		}
		wg.Done()
}

// Handles incoming requests.

var mutex = &sync.Mutex{}
func handleRequest(conn connection, chans []chan string) {
	// Make a buffer to hold incoming data.
	//fmt.Println("New connection added to server with: "+ conn.name)
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

		// This may or may not be necessary, since when we receive this text, 
		// especially for own messages

		// will have to figure out where we need to query the hold back queue messages at
		keep := true

		if (!isMyOld && !isOld) {
			// this message has never been sent by me
			// it has also never been sent by anyone else to me
			// this is the first time i am actually receiveing this
			// so i need to figure out if i should keep this message or put it in a holdback queue

			// since this is the first time im receiving it, it is an event
			// so i will increment by timestamp
			m := str_to_map(words[1])
			keep := now_or_later(m, words[2])
			
			//keep = now_or_later(m)

			if keep {
				fmt.Println("do the usual shit")
				// I have decided to keep this message
				// so I will have to print it and add it to my list of all messages
				// received for the first time hence send to all other servers
				update_timestamps(m, words[2])
				words[1] = map_to_str(VecTimestamp) 
				for _, c := range chans {
					c <- text
				}	
				
				// check if any buffered messages can now be delivered
				deliver_buf_msgs(chans)

			} else {
				fmt.Println("Put it away for later")
				// I have decided to put it away for later
				// I will deal with this later
				// implement hold back queue
				outOfOrderMsgs[words[0]] = text

			}
		}

		if (!isOld && keep) {
			// this is a message I just sent
			// or a message that should be kept that was delivered by another process
		
			msg := strings.Join(words[3:], " ")
			fmt.Print(words[1] + " ")
			fmt.Printf("%v\n", msg);
		
			allMessages[words[0]] = text
		}

		mutex.Unlock()	
	}

	// Close the connection when you're done with it.
	conn.conn.Close()
}


func deliver_buf_msgs(chans []chan string) {
	// iterate over them and run them through the now_or_later func
	// then execute the (!isOld && keep) condition to print these messages
	// that have just been delivered

	delivered_keys := make([]string, 0)
	for key, text := range outOfOrderMsgs {
		words := strings.Fields(text)
		m := str_to_map(words[1])
		keep := now_or_later(m, words[2])
		if keep {
			// yay, the buffered message can finally be delivered
			update_timestamps(m, words[2])
			words[1] = map_to_str(VecTimestamp) 
			for _, c := range chans {
				c <- text
			}
			msg := strings.Join(words[3:], " ")	
			fmt.Print(words[1] + " ")
			fmt.Printf("%v\n", msg);
			allMessages[words[0]] = text	
			delivered_keys = append(delivered_keys, key)
		}	
	}
	if len(delivered_keys) > 0 {
		// remove the msgs that got delivered and repeat the process
		for _, key := range delivered_keys {
			delete(outOfOrderMsgs, key)
		} 
		deliver_buf_msgs(chans)
	} 
}


func now_or_later(m map[string]int, rem_addr string) bool {

	if m[rem_addr] != (VecTimestamp[rem_addr] + 1) {
		fmt.Println("basic check failed")
		return false
	}
	
	for key, val := range m {
		if ((key != rem_addr) && (val > VecTimestamp[key])) {
			fmt.Println("something internal failed")
			return false
		} 
	}

	return true
}

func update_timestamps(m map[string]int, rem_addr string) {
	// update own value because receive is an event
	//VecTimestamp[vm_num]++

	VecTimestamp[rem_addr] = m[rem_addr]
}

func client(conn net.Conn, c chan string) {

	fmt.Fprintf(conn, name)
	for {
		text := <- c
		// words := strings.Fields(text)
		// read in input from stdin
		// msg := message{name, text}
		// send to socket
		fmt.Fprintf(conn, text)
	}

}
