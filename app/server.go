package main

import (
    "encoding/binary"
    "encoding/hex"
    "fmt"
    "io"
    "net"
    "os"
    "sync/atomic"
    "time"

    "github.com/spf13/viper"
    "go.uber.org/zap"
)

// Original types from previous implementation
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

// Constants from original implementation
const (
    API_VERSION_REQUEST uint16 = 18
    FETCH_REQUEST       uint16 = 1

    ERROR_UNSUPPORTED_VERSION = 35
    ERROR_UNKNOWN_TOPICID     = 100
)

// API keys configuration
var apiKeys = []APIKey{
    {
        Key:         18,
        MinVersion:  0,
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

// Config holds all server configuration
type Config struct {
    Host            string
    Port            string
    ReadTimeout     time.Duration
    WriteTimeout    time.Duration
    MaxMessageSize  int
    MaxConnections  int
    LogLevel        string
    MetricsInterval time.Duration
}

// ServerMetrics tracks server-wide metrics
type ServerMetrics struct {
    ActiveConnections int64
    TotalRequests     int64
    TotalErrors       int64
    BytesReceived     int64
    BytesSent         int64
}

// ConnectionMetrics tracks per-connection metrics
type ConnectionMetrics struct {
    RequestCount  int64
    ErrorCount    int64
    BytesReceived int64
    BytesSent     int64
    ConnectedAt   time.Time
    LastRequestAt time.Time
    ClientAddr    string
    ClientID      string
}

// FetchResponseBuilder from original implementation
type FetchResponseBuilder struct {
    response []byte
}

func NewFetchResponseBuilder() *FetchResponseBuilder {
    return &FetchResponseBuilder{
        response: make([]byte, 0),
    }
}

func (b *FetchResponseBuilder) addHeader(correlationID uint32) {
    b.response = binary.BigEndian.AppendUint32(b.response, correlationID)
    b.response = append(b.response, 0)
    b.response = binary.BigEndian.AppendUint32(b.response, 0)
    b.response = binary.BigEndian.AppendUint16(b.response, 0)
    b.response = binary.BigEndian.AppendUint32(b.response, 0)
}

func (b *FetchResponseBuilder) addTopicResponse(topicID []byte) {
    b.response = append(b.response, byte(2))
    b.response = append(b.response, topicID...)
}

func (b *FetchResponseBuilder) addPartitionInfo(topicID []byte) {
    b.response = append(b.response, byte(2))
    b.response = binary.BigEndian.AppendUint32(b.response, 0)

    uuid := hex.EncodeToString(topicID)
    if uuid[12:16] == "4000" {
        b.response = binary.BigEndian.AppendUint16(b.response, 0)
    } else {
        b.response = binary.BigEndian.AppendUint16(b.response, ERROR_UNKNOWN_TOPICID)
    }
}

func (b *FetchResponseBuilder) addPartitionMetadata() {
    b.response = binary.BigEndian.AppendUint64(b.response, 0)
    b.response = binary.BigEndian.AppendUint64(b.response, 0)
    b.response = binary.BigEndian.AppendUint64(b.response, 0)
    b.response = binary.BigEndian.AppendUint32(b.response, 0)
    b.response = binary.BigEndian.AppendUint16(b.response, 0)
    b.response = binary.BigEndian.AppendUint16(b.response, 0)
}

func (b *FetchResponseBuilder) addEmptyResponse() {
    b.response = append(b.response, byte(1))
}

func (b *FetchResponseBuilder) build() []byte {
    b.response = append(b.response, 0)
    msgLen := make([]byte, 4)
    binary.BigEndian.PutUint32(msgLen, uint32(len(b.response)))
    return append(msgLen, b.response...)
}

type Server struct {
    config   *Config
    logger   *zap.Logger
    metrics  *ServerMetrics
    listener net.Listener
}

// Response handlers from original implementation
func (s *Server) createApiVersionsResponse(correlationID uint32, errorCode int16) []byte {
    response := []byte{}

    response = binary.BigEndian.AppendUint32(response, correlationID)
    response = binary.BigEndian.AppendUint16(response, uint16(errorCode))

    if errorCode == 0 {
        response = append(response, 3)

        response = binary.BigEndian.AppendUint16(response, API_VERSION_REQUEST)
        response = binary.BigEndian.AppendUint16(response, 0)
        response = binary.BigEndian.AppendUint16(response, 4)
        response = append(response, 0)

        response = binary.BigEndian.AppendUint16(response, FETCH_REQUEST)
        response = binary.BigEndian.AppendUint16(response, 0)
        response = binary.BigEndian.AppendUint16(response, 16)
        response = append(response, 0)

        response = binary.BigEndian.AppendUint32(response, 0)
        response = append(response, 0)
    }

    size := make([]byte, 4)
    binary.BigEndian.PutUint32(size, uint32(len(response)))
    return append(size, response...)
}

func (s *Server) handleFetchResponse(request *Message) []byte {
    builder := NewFetchResponseBuilder()
    builder.addHeader(request.CorrelationID)

    topicsLength := binary.BigEndian.Uint16(request.Body[21:23])
    if topicsLength > 1 {
        topicID := request.Body[23:39]
        builder.addTopicResponse(topicID)
        builder.addPartitionInfo(topicID)
        builder.addPartitionMetadata()
    } else {
        builder.addEmptyResponse()
    }

    return builder.build()
}

// loadConfig loads configuration from environment variables and config file
func loadConfig() (*Config, error) {
    v := viper.New()

    // Set defaults
    v.SetDefault("host", "0.0.0.0")
    v.SetDefault("port", "9092")
    v.SetDefault("read_timeout", "30s")
    v.SetDefault("write_timeout", "30s")
    v.SetDefault("max_message_size", 1048576) // 1MB
    v.SetDefault("max_connections", 1000)
    v.SetDefault("log_level", "info")
    v.SetDefault("metrics_interval", "60s")

    // Environment variables
    v.AutomaticEnv()
    v.SetEnvPrefix("KAFKA_SERVER")

    // Optional config file
    v.SetConfigName("config")
    v.SetConfigType("yaml")
    v.AddConfigPath(".")
    v.AddConfigPath("/etc/kafka-server/")

    if err := v.ReadInConfig(); err != nil {
        if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
            return nil, fmt.Errorf("error reading config file: %w", err)
        }
    }

    config := &Config{
        Host:            v.GetString("host"),
        Port:            v.GetString("port"),
        ReadTimeout:     v.GetDuration("read_timeout"),
        WriteTimeout:    v.GetDuration("write_timeout"),
        MaxMessageSize:  v.GetInt("max_message_size"),
        MaxConnections:  v.GetInt("max_connections"),
        LogLevel:        v.GetString("log_level"),
        MetricsInterval: v.GetDuration("metrics_interval"),
    }

    return config, nil
}

// initLogger creates a new zap logger instance
func initLogger(logLevel string) (*zap.Logger, error) {
    config := zap.NewProductionConfig()

    level, err := zap.ParseAtomicLevel(logLevel)
    if err != nil {
        return nil, fmt.Errorf("invalid log level: %w", err)
    }
    config.Level = level

    logger, err := config.Build()
    if err != nil {
        return nil, fmt.Errorf("error building logger: %w", err)
    }

    return logger, nil
}

// NewServer creates a new server instance
func NewServer() (*Server, error) {
    config, err := loadConfig()
    if err != nil {
        return nil, fmt.Errorf("error loading config: %w", err)
    }

    logger, err := initLogger(config.LogLevel)
    if err != nil {
        return nil, fmt.Errorf("error initializing logger: %w", err)
    }

    metrics := &ServerMetrics{}

    return &Server{
        config:  config,
        logger:  logger,
        metrics: metrics,
    }, nil
}

// startMetricsReporting periodically logs server metrics
func (s *Server) startMetricsReporting() {
    ticker := time.NewTicker(s.config.MetricsInterval)
    go func() {
        for range ticker.C {
            s.logger.Info("server metrics",
                zap.Int64("active_connections", atomic.LoadInt64(&s.metrics.ActiveConnections)),
                zap.Int64("total_requests", atomic.LoadInt64(&s.metrics.TotalRequests)),
                zap.Int64("total_errors", atomic.LoadInt64(&s.metrics.TotalErrors)),
                zap.Int64("bytes_received", atomic.LoadInt64(&s.metrics.BytesReceived)),
                zap.Int64("bytes_sent", atomic.LoadInt64(&s.metrics.BytesSent)),
            )
        }
    }()
}

// handleClient handles a single client connection
func (s *Server) handleClient(conn net.Conn) {
    defer conn.Close()

    metrics := &ConnectionMetrics{
        ConnectedAt: time.Now(),
        ClientAddr:  conn.RemoteAddr().String(),
    }

    atomic.AddInt64(&s.metrics.ActiveConnections, 1)
    defer atomic.AddInt64(&s.metrics.ActiveConnections, -1)

    s.logger.Info("client connected",
        zap.String("client_addr", metrics.ClientAddr),
        zap.Time("connected_at", metrics.ConnectedAt),
    )

    for {
        if err := conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)); err != nil {
            s.logger.Error("error setting read deadline", zap.Error(err))
            return
        }

        requestMessage, err := s.readMessage(conn, metrics)
        if err != nil {
            if err == io.EOF {
                s.logger.Info("client disconnected",
                    zap.String("client_addr", metrics.ClientAddr),
                    zap.Int64("total_requests", metrics.RequestCount),
                )
                return
            }
            s.logger.Error("error reading message",
                zap.Error(err),
                zap.String("client_addr", metrics.ClientAddr),
            )
            atomic.AddInt64(&s.metrics.TotalErrors, 1)
            atomic.AddInt64(&metrics.ErrorCount, 1)
            return
        }

        metrics.LastRequestAt = time.Now()
        atomic.AddInt64(&metrics.RequestCount, 1)
        atomic.AddInt64(&s.metrics.TotalRequests, 1)

        if requestMessage.ClientId != nil {
            metrics.ClientID = *requestMessage.ClientId
        }

        var response []byte
        switch requestMessage.APIKey {
        case API_VERSION_REQUEST:
            response = s.createApiVersionsResponse(requestMessage.CorrelationID, requestMessage.Error)
        case FETCH_REQUEST:
            response = s.handleFetchResponse(requestMessage)
        default:
            s.logger.Warn("unsupported API key",
                zap.Uint16("api_key", requestMessage.APIKey),
                zap.String("client_addr", metrics.ClientAddr),
            )
            atomic.AddInt64(&s.metrics.TotalErrors, 1)
            atomic.AddInt64(&metrics.ErrorCount, 1)
            return
        }

        if err := conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
            s.logger.Error("error setting write deadline", zap.Error(err))
            return
        }

        n, err := conn.Write(response)
        if err != nil {
            s.logger.Error("error sending response",
                zap.Error(err),
                zap.String("client_addr", metrics.ClientAddr),
            )
            atomic.AddInt64(&s.metrics.TotalErrors, 1)
            atomic.AddInt64(&metrics.ErrorCount, 1)
            return
        }

        atomic.AddInt64(&metrics.BytesSent, int64(n))
        atomic.AddInt64(&s.metrics.BytesSent, int64(n))
    }
}

// readMessage reads and parses a message from the connection
func (s *Server) readMessage(conn net.Conn, metrics *ConnectionMetrics) (*Message, error) {
    buffer := make([]byte, s.config.MaxMessageSize)
    n, err := conn.Read(buffer)
    if err != nil {
        return nil, err
    }

    atomic.AddInt64(&metrics.BytesReceived, int64(n))
    atomic.AddInt64(&s.metrics.BytesReceived, int64(n))

    clientIDLength := int16(binary.BigEndian.Uint16(buffer[12:14]))
    var clientID *string
    if clientIDLength != -1 {
        clientIDBytes := buffer[14 : 14+clientIDLength]
        clientIDStr := string(clientIDBytes)
        clientID = &clientIDStr
    }

    apiKey := binary.BigEndian.Uint16(buffer[4:6])
    version := binary.BigEndian.Uint16(buffer[6:8])

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
        Body:          buffer[14+clientIDLength : n],
        Error:         errorCode,
    }, nil
}

func main() {
    server, err := NewServer()
    if err != nil {
        fmt.Printf("Failed to create server: %v\n", err)
        os.Exit(1)
    }
    defer server.logger.Sync()

    addr := fmt.Sprintf("%s:%s", server.config.Host, server.config.Port)
    listener, err := net.Listen("tcp", addr)
    if err != nil {
        server.logger.Fatal("failed to bind to port",
            zap.String("addr", addr),
            zap.Error(err),
        )
    }
    defer listener.Close()

    server.listener = listener
    server.startMetricsReporting()

    server.logger.Info("server started",
        zap.String("addr", addr),
        zap.String("log_level", server.config.LogLevel),
    )

    for {
        conn, err := listener.Accept()
        if err != nil {
            server.logger.Error("error accepting connection", zap.Error(err))
            continue
        }

        if atomic.LoadInt64(&server.metrics.ActiveConnections) >= int64(server.config.MaxConnections) {
            server.logger.Warn("max connections reached, rejecting connection",
                zap.String("client_addr", conn.RemoteAddr().String()),
            )
            conn.Close()
            continue
        }

        go server.handleClient(conn)
    }
}
