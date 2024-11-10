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
type Message struct {
	Error int16
	CorrelationID int32
}

// creating constant
const (
	ErrUnsupportedVersion int16 = 35
	DefaultPort                 = "9092"
	MinSupportedVersion         = 3
	MaxSupportedVersion         = 4

)


func readMessage(conn net.Conn) (*Message, error) {
	var tmp32, length int32
	var tmp16 int16

	out := Message{}

	// Header
	binary.Read(conn, binary.BigEndian, &length)
	binary.Read(conn, binary.BigEndian, &tmp16)
	binary.Read(conn, binary.BigEndian, &tmp16)
	binary.Read(conn, binary.BigEndian, &tmp32)
	out.CorrelationID = tmp32

	if tmp16 < 0 || tmp16 > 4 {
		out.Error = ErrUnsupportedVersion
	}

	// Body
	tmp := make([]byte, length-8)
	conn.Read(tmp)

	return &out, nil
}

func sendResponse(c net.Conn, b []byte) error {
	binary.Write(c, binary.BigEndian, int32(len(b)))
	binary.Write(c, binary.BigEndian, b)

	return nil
}

func main() {

	 l, err := net.Listen("tcp", "0.0.0.0:" + DefaultPort)
	 if err != nil {
		fmt.Println("Failed to bind to port ", DefaultPort)
	 	os.Exit(1)
	 }

	
	fmt.Println("Server started and waiting for connections...")
	
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection:", err.Error())
		os.Exit(1)
	}

	m, err := readMessage(conn)
	if err != nil {
		fmt.Println("Error reading message:", err.Error())
		os.Exit(1)
	}

	if m.Error !=0 {
		out := make([]byte, 6)
		binary.BigEndian.PutUint32(out, uint32(m.CorrelationID))
		binary.BigEndian.PutUint16(out[4:], uint16(m.Error))
		sendResponse(conn, out)
		os.Exit(1)
	
	}

	out := make([]byte, 19)
	binary.BigEndian.PutUint32(out, uint32(m.CorrelationID))
	binary.BigEndian.PutUint16(out[4:], 0) //Error Code

	out[6] = 2                             //Number of API Keys
	binary.BigEndian.PutUint16(out[7:], 18)//API Key 1 - API_VERSIONS
	binary.BigEndian.PutUint16(out[9:], MinSupportedVersion) // min version
	binary.BigEndian.PutUint16(out[11:], MaxSupportedVersion)// max version

	out[13] = 0                            // _tagged_fields
	binary.BigEndian.PutUint32(out[14:], 0)// throttle time
	out[18]  = 0

	sendResponse(conn, out)
	
}

