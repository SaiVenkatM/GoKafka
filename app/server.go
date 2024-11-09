package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit
type RequestHeader struct {
	messageSize int32
	apiKey int16
	apiVersion int16
	correlationID int32
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	
	 l, err := net.Listen("tcp", "0.0.0.0:9092")
	 if err != nil {
		fmt.Println("Failed to bind to port 9092")
	 	os.Exit(1)
	 }

	
	fmt.Println("Server started and waiting for connections...")
	
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			os.Exit(1)
		}

		defer conn.Close()

		buf := make([]byte, 1024)
		_, err = conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading:", err)
			os.Exit(1)
		}


		response := make([]byte, 8)
		//correlationID := binary.BigEndian.Uint32(buf[8:12])
		requestApiVersion := binary.BigEndian.Uint16(buf[6:8])
		copy(response[4:8], buf[8:12])
		if  requestApiVersion < 0 || requestApiVersion > 4 {
			response =   append(response, 0, 35)
		} else {
			response = append(response, 0, 0)
		}
		_, err = conn.Write(response)
		if err != nil {
			fmt.Println("Error writing response:", err)
			os.Exit(1)
		}	

	
}

