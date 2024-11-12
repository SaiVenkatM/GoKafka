package main

import (
    "encoding/binary"
    "fmt"
    "io"
    "net"
    "os"
)

type APIKey struct {
    Key         uint16
    MinVersion  uint16
    MaxVersion  uint16
    TaggedField byte
}

const (
    ErrUnsupportedVersion int16 = 35
    DefaultPort                 = "9092"
    DefaultAddr                 = "0.0.0.0"
    ErrUnknown            int16 = 0
)

const (
    API_VERSION_REQUEST uint16 = 18
    FETCH_REQUEST       uint16 = 1

    ERROR_UNSUPPORTED_VERSION = 35
)

type Message struct {
    CorrelationID int32
    APIKey        int16
    Version       int16
    Error         int16
}

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
    // Read message size
    messageSizeField := make([]byte, 4)
    _, err := io.ReadFull(conn, messageSizeField)
    if err != nil {
        if err == io.EOF {
            return nil, err
        }
        return nil, fmt.Errorf("error reading message size: %v", err)
    }

    // Read the complete message
    messageSize := binary.BigEndian.Uint32(messageSizeField)
    message := make([]byte, messageSize)
    _, err = io.ReadFull(conn, message)
    if err != nil {
        return nil, fmt.Errorf("error reading message: %v", err)
    }

    // Parse header fields
    apiKey := binary.BigEndian.Uint16(message[0:2])
    version := int16(binary.BigEndian.Uint16(message[2:4]))
    correlationID := binary.BigEndian.Uint32(message[4:8])

    out := &Message{
        APIKey:        int16(apiKey),
        CorrelationID: int32(correlationID),
        Version:       version,
    }

    // Check version support for API Versions request
    if apiKey == API_VERSION_REQUEST {
        if version < 0 || version > 4 {
            out.Error = ERROR_UNSUPPORTED_VERSION
        }
    }

    return out, nil
}

func handleFetchResponse(correlationID int32) []byte {
    response := []byte{}

    // Correlation ID (4 bytes)
    response = binary.BigEndian.AppendUint32(response, uint32(correlationID))

    // TAG_BUFFER
    response = append(response, 0)

    // Throttle time (4 bytes)
    response = binary.BigEndian.AppendUint32(response, 0)

    // Error code (2 bytes)
    response = binary.BigEndian.AppendUint16(response, 0)

    // Session ID (4 bytes)
    response = binary.BigEndian.AppendUint32(response, 0)

    // Responses array length (0)
    response = append(response, 0)

    // Final TAG_BUFFER
    response = append(response, 0)

    // Prepend size
    size := make([]byte, 4)
    binary.BigEndian.PutUint32(size, uint32(len(response)))
    return append(size, response...)
}

func createApiVersionsResponse(correlationID int32, errorCode int16) []byte {
    response := []byte{}

    // Correlation ID
    response = binary.BigEndian.AppendUint32(response, uint32(correlationID))

    // Error code
    response = binary.BigEndian.AppendUint16(response, uint16(errorCode))

    if errorCode == 0 {
        // Number of API keys
        response = append(response, 3)

        // API Version Key entry
        response = binary.BigEndian.AppendUint16(response, API_VERSION_REQUEST)
        response = binary.BigEndian.AppendUint16(response, 0) // Min version
        response = binary.BigEndian.AppendUint16(response, 4) // Max version
        response = append(response, 0)                        // TAG_BUFFER

        // Fetch API Key entry
        response = binary.BigEndian.AppendUint16(response, FETCH_REQUEST)
        response = binary.BigEndian.AppendUint16(response, 0)  // Min version
        response = binary.BigEndian.AppendUint16(response, 16) // Max version
        response = append(response, 0)                         // TAG_BUFFER

        // Throttle time
        response = binary.BigEndian.AppendUint32(response, 0)

        // Final TAG_BUFFER
        response = append(response, 0)
    }

    // Prepend size
    size := make([]byte, 4)
    binary.BigEndian.PutUint32(size, uint32(len(response)))
    return append(size, response...)
}

func handleClient(conn net.Conn) {
    defer conn.Close()
    fmt.Printf("New client connected: %s\n", conn.RemoteAddr().String())

    for {
        msg, err := readMessage(conn)
        if err != nil {
            if err == io.EOF {
                fmt.Printf("Client disconnected: %s\n", conn.RemoteAddr().String())
                return
            }
            fmt.Printf("Error reading message: %v\n", err)
            return
        }

        var response []byte
        switch uint16(msg.APIKey) {
        case API_VERSION_REQUEST:
            response = createApiVersionsResponse(msg.CorrelationID, msg.Error)
        case FETCH_REQUEST:
            response = handleFetchResponse(msg.CorrelationID)
        default:
            fmt.Printf("Unsupported API Key: %d\n", msg.APIKey)
            return
        }

        _, err = conn.Write(response)
        if err != nil {
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
