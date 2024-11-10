package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"io"
)

// Message struct and constants remain the same
type Message struct {
	Error         int16
	CorrelationID int32
}

const (
	ErrUnsupportedVersion int16  = 35
	DefaultPort                  = "9092"
	MinSupportedVersion         = 3
	MaxSupportedVersion         = 4
	DefaultAddr                 = "0.0.0.0"
)

func readMessage(conn net.Conn) (*Message, error) {
	var tmp32, length int32
	var tmp16 int16

	out := Message{}

	// Read length with error handling
	err := binary.Read(conn, binary.BigEndian, &length)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("error reading message length: %v", err)
	}

	// Header
	if err := binary.Read(conn, binary.BigEndian, &tmp16); err != nil {
		return nil, fmt.Errorf("error reading header field 1: %v", err)
	}
	if err := binary.Read(conn, binary.BigEndian, &tmp16); err != nil {
		return nil, fmt.Errorf("error reading header field 2: %v", err)
	}
	if err := binary.Read(conn, binary.BigEndian, &tmp32); err != nil {
		return nil, fmt.Errorf("error reading correlation ID: %v", err)
	}
	out.CorrelationID = tmp32

	if tmp16 < 0 || tmp16 > 4 {
		out.Error = ErrUnsupportedVersion
	}

	// Body
	tmp := make([]byte, length-8)
	_, err = io.ReadFull(conn, tmp)
	if err != nil {
		return nil, fmt.Errorf("error reading message body: %v", err)
	}

	return &out, nil
}

func sendResponse(c net.Conn, b []byte) error {
	// Write message length
	if err := binary.Write(c, binary.BigEndian, int32(len(b))); err != nil {
		return fmt.Errorf("error writing response length: %v", err)
	}
	// Write message content
	if err := binary.Write(c, binary.BigEndian, b); err != nil {
		return fmt.Errorf("error writing response body: %v", err)
	}
	return nil
}

func handleClient(conn net.Conn) {
	defer conn.Close()
	
	fmt.Printf("New client connected: %s\n", conn.RemoteAddr().String())

	for {
		// Read message
		m, err := readMessage(conn)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("Client disconnected: %s\n", conn.RemoteAddr().String())
				return
			}
			fmt.Printf("Error reading message from %s: %v\n", conn.RemoteAddr().String(), err)
			return
		}

		// Handle error case
		if m.Error != 0 {
			out := make([]byte, 6)
			binary.BigEndian.PutUint32(out, uint32(m.CorrelationID))
			binary.BigEndian.PutUint16(out[4:], uint16(m.Error))
			if err := sendResponse(conn, out); err != nil {
				fmt.Printf("Error sending error response: %v\n", err)
				return
			}
			continue
		}

		// Prepare and send normal response
		out := make([]byte, 19)
		binary.BigEndian.PutUint32(out, uint32(m.CorrelationID))
		binary.BigEndian.PutUint16(out[4:], 0)    // Error Code
		out[6] = 2                                 // Number of API Keys
		binary.BigEndian.PutUint16(out[7:], 18)   // API Key 1 - API_VERSIONS
		binary.BigEndian.PutUint16(out[9:], MinSupportedVersion)
		binary.BigEndian.PutUint16(out[11:], MaxSupportedVersion)
		out[13] = 0                               // _tagged_fields
		binary.BigEndian.PutUint32(out[14:], 0)   // throttle time
		out[18] = 0

		if err := sendResponse(conn, out); err != nil {
			fmt.Printf("Error sending response: %v\n", err)
			return
		}
	}
}

func main() {
	l, err := net.Listen("tcp", DefaultAddr+":"+DefaultPort)
	if err != nil {
		fmt.Printf("Failed to bind to port %s: %v\n", DefaultPort, err)
		os.Exit(1)
	}
	defer l.Close()

	fmt.Printf("Server started and listening on %s:%s\n", DefaultAddr, DefaultPort)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}
		
		// Handle each client in its own goroutine
		go handleClient(conn)
	}
}