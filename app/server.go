package main

import (
    "encoding/binary"
    "fmt"
    "net"
    "os"
    "io"
)

type Message struct {
    Error         int16
    CorrelationID int32
}

// Added new struct to represent API Keys
type APIKey struct {
    Key         uint16
    MinVersion  uint16
    MaxVersion  uint16
    TaggedField byte
}

const (
    ErrUnsupportedVersion int16  = 35
    DefaultPort                  = "9092"
    DefaultAddr                 = "0.0.0.0"
)

// Predefined API keys
var apiKeys = []APIKey{
    {
        Key:         18, // APIVersions
        MinVersion:  3,
        MaxVersion:  4,
        TaggedField: 0,
    },
    {
        Key:         75, // DescribeTopicPartitions
        MinVersion:  0,
        MaxVersion:  0,
        TaggedField: 0,
    },
    {
		Key:         1, //  Fetch API
		MinVersion:  0,
		MaxVersion:  16,
		TaggedField: 0,
	},
}

func readMessage(conn net.Conn) (*Message, error) {
    var tmp32, length int32
    var tmp16 int16

    out := Message{}

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

func createResponse(correlationID int32) []byte {
    // Calculate total size needed for the response
    totalSize := 4 + // Correlation ID
                2 + // Error Code
                1 + // Number of API Keys
                (len(apiKeys) * 7) + // Each API Key entry (2+2+2+1 bytes)
                4 + // Throttle time
                1   // Final tagged fields

    out := make([]byte, totalSize)
    offset := 0

    // Write correlation ID
    binary.BigEndian.PutUint32(out[offset:], uint32(correlationID))
    offset += 4

    // Write error code (0 = no error)
    binary.BigEndian.PutUint16(out[offset:], 0)
    offset += 2

    // Write number of API keys
    out[offset] = byte(len(apiKeys)) + 1 // Add 1 as per format
    offset++

    // Write each API key entry
    for _, api := range apiKeys {
        binary.BigEndian.PutUint16(out[offset:], api.Key)
        offset += 2
        binary.BigEndian.PutUint16(out[offset:], api.MinVersion)
        offset += 2
        binary.BigEndian.PutUint16(out[offset:], api.MaxVersion)
        offset += 2
        out[offset] = api.TaggedField
        offset++
    }

    // Write throttle time (4 bytes of zeros)
    binary.BigEndian.PutUint32(out[offset:], 0)
    offset += 4

    // Write final tagged fields
    out[offset] = 0

    return out
}

func sendResponse(c net.Conn, b []byte) error {
    if err := binary.Write(c, binary.BigEndian, int32(len(b))); err != nil {
        return fmt.Errorf("error writing response length: %v", err)
    }
    if err := binary.Write(c, binary.BigEndian, b); err != nil {
        return fmt.Errorf("error writing response body: %v", err)
    }
    return nil
}

func handleClient(conn net.Conn) {
    defer conn.Close()

    fmt.Printf("New client connected: %s\n", conn.RemoteAddr().String())

    for {
        m, err := readMessage(conn)
        if err != nil {
            if err == io.EOF {
                fmt.Printf("Client disconnected: %s\n", conn.RemoteAddr().String())
                return
            }
            fmt.Printf("Error reading message from %s: %v\n", conn.RemoteAddr().String(), err)
            return
        }

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

        response := createResponse(m.CorrelationID)
        if err := sendResponse(conn, response); err != nil {
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
        go handleClient(conn)
    }
}