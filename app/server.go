package main

import (
    "encoding/binary"
    "encoding/hex"
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

type Message struct {
    MessageLength uint32
    CorrelationID uint32
    APIKey        uint16
    Version       uint16
    Error         int16
    ClientId      *string
    Body          []byte
}

type Topic struct {
    TopicID    []byte
    Partitions []Partition
}

type Partition struct {
    PartitionIdx int32
    ErrorCode    int16
}

type FetchResponse struct {
    Throttle_time_ms int32
    Error_code       int16
    Session_id       int32
    Responses        []Topic
}

// Update API keys to match version 1's format
var apiKeys = []APIKey{
    {
        Key:         18,
        MinVersion:  0, // Changed to support all versions 0-4
        MaxVersion:  4,
        TaggedField: 0,
    },
    {
        Key:         1,
        MinVersion:  0,
        MaxVersion:  16,
        TaggedField: 0,
    },
}

// Keep existing constants but modify supported versions
const (
    DefaultPort = "9092"
    DefaultAddr = "0.0.0.0"

    API_VERSION_REQUEST uint16 = 18
    FETCH_REQUEST       uint16 = 1
    BYTE_SIZE                  = 1024

    ERROR_UNSUPPORTED_VERSION = 35
    ERROR_UNKNOWN_TOPICID     = 100
)

// Modified createApiVersionsResponse to match version 1's format
func createApiVersionsResponse(correlationID uint32, errorCode int16) []byte {
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

// Modified handleFetchResponse to match version 1's format
func handleFetchResponse(request *Message) []byte {
    response := make([]byte, 0)

    // Correlation ID (4 bytes)
    correlationIDBytes := make([]byte, 4)
    binary.BigEndian.PutUint32(correlationIDBytes, request.CorrelationID)
    response = append(response, correlationIDBytes...)

    // Tag buffer (1 byte)
    response = append(response, 0)

    // Throttle time (4 bytes)
    response = binary.BigEndian.AppendUint32(response, 0)

    // Error code (2 bytes)
    response = binary.BigEndian.AppendUint16(response, 0)

    // Session id (4 bytes)
    response = binary.BigEndian.AppendUint32(response, 0)

    // Parse topic information from request
    topicsLength := binary.BigEndian.Uint16(request.Body[21:23])
    if topicsLength > 1 {
        // Number of responses (1 byte)
        response = append(response, byte(2)) // 1 response + 1 for length

        // Topic ID (16 bytes)
        topicID := request.Body[23:39]
        response = append(response, topicID...)

        // Number of partitions (1 byte)
        response = append(response, byte(2)) // 1 partition + 1 for length

        // Partition info
        partIndexBytes := make([]byte, 4)
        binary.BigEndian.PutUint32(partIndexBytes, 0)
        response = append(response, partIndexBytes...)

        // Partition error code (2 bytes)
        partErrorBytes := make([]byte, 2)
        uuid := hex.EncodeToString(topicID)

        if uuid[12:16] == "4000" {
            binary.BigEndian.PutUint16(partErrorBytes, 0)

        } else {
            binary.BigEndian.PutUint16(partErrorBytes, ERROR_UNKNOWN_TOPICID)

        }
        response = append(response, partErrorBytes...)

        // Add additional required fields
        // High watermark (8 bytes)
        response = binary.BigEndian.AppendUint64(response, 0)
        // Last stable offset (8 bytes)
        response = binary.BigEndian.AppendUint64(response, 0)
        // Log start offset (8 bytes)
        response = binary.BigEndian.AppendUint64(response, 0)
        // Preferred read replica (4 bytes)
        response = binary.BigEndian.AppendUint32(response, 0)

        // Partition tag buffer (2 bytes)
        response = binary.BigEndian.AppendUint16(response, 0)

        // Response tag buffer (2 bytes)
        response = binary.BigEndian.AppendUint16(response, 0)
    } else {
        // No responses
        response = append(response, byte(1))
    }

    // Final tag buffer (1 byte)
    response = append(response, 0)

    // Prepend message length
    msgLen := make([]byte, 4)
    binary.BigEndian.PutUint32(msgLen, uint32(len(response)))
    return append(msgLen, response...)
}

// Update readMessage to check version support like version 1
func readMessage(conn net.Conn) (*Message, error) {
    buffer := make([]byte, BYTE_SIZE)
    _, err := conn.Read(buffer)
    if err != nil {
        return nil, fmt.Errorf("error reading from client: %w", err)
    }

    clientIDLength := int16(binary.BigEndian.Uint16(buffer[12:14]))
    var clientID *string
    if clientIDLength != -1 {
        clientIDBytes := buffer[14 : 14+clientIDLength]
        clientIDStr := string(clientIDBytes)
        clientID = &clientIDStr
    }

    apiKey := binary.BigEndian.Uint16(buffer[4:6])
    version := binary.BigEndian.Uint16(buffer[6:8])

    // Version check for API Versions request
    var errorCode int16 = 0
    if apiKey == API_VERSION_REQUEST && version > 4 {
        errorCode = ERROR_UNSUPPORTED_VERSION
    }

    return &Message{
        MessageLength: binary.BigEndian.Uint32(buffer[0:4]),
        APIKey:        apiKey,
        Version:       version,
        CorrelationID: binary.BigEndian.Uint32(buffer[8:12]),
        ClientId:      clientID,
        Body:          buffer[14+clientIDLength:],
        Error:         errorCode,
    }, nil
}

// Rest of the code (handleClient, main) can remain the same

func handleClient(conn net.Conn) {
    defer conn.Close()
    fmt.Printf("New client connected: %s\n", conn.RemoteAddr().String())

    for {
        requestMessage, err := readMessage(conn)
        if err != nil {
            if err == io.EOF {
                fmt.Printf("Client disconnected: %s\n", conn.RemoteAddr().String())
                return
            }
            fmt.Printf("Error reading message: %v\n", err)
            return
        }

        var response []byte
        //finalResponse := requestMessage.CorrelationID
        switch uint16(requestMessage.APIKey) {
        case API_VERSION_REQUEST:
            response = createApiVersionsResponse(requestMessage.CorrelationID, requestMessage.Error)
        case FETCH_REQUEST:
            response = handleFetchResponse(requestMessage)
        default:
            fmt.Printf("Unsupported API Key: %d\n", requestMessage.APIKey)
            return
        }

        //finalResponse = append(finalResponse, response...)

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
