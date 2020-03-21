package config

import (
	"crypto/md5"
	"crypto/tls"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	"go.uber.org/zap"
)

type Config struct {
	// basic options
	NodeID int64 `yaml:"nodeID"`
	Logger *zap.Logger

	TCPAddress               string        `yaml:"tcpAddress"`
	HTTPAddress              string        `yaml:"httpAddress"`
	HTTPSAddress             string        `yaml:"httpsAddress"`
	HTTPClientConnectTimeout time.Duration `yaml:"httpClientConnectTimeout"`
	HTTPClientRequestTimeout time.Duration `yaml:"httpClientRequestTimeout"`

	// msg and command options
	MsgTimeout       time.Duration `yaml:"msgTimeout"`
	MaxMsgTimeout    time.Duration `yaml:"maxMsgTimeout"`
	MaxMsgSize       int64         `yaml:"maxMsgSize"`
	MaxBodySize      int64         `yaml:"maxBodySize"`
	MaxReqTimeout    time.Duration `yaml:"maxReqTimeout"`
	HeartbeatTimeout time.Duration `yaml:"heartbeatTimeout"`

	// client overridable configuration options
	MaxHeartbeatInterval   time.Duration `yaml:"maxHeartbeatInterval"`
	MaxOutputBufferSize    int64         `yaml:"maxOutputBufferSize"`
	MaxOutputBufferTimeout time.Duration `yaml:"maxOutputBufferTimeout"`
	MinOutputBufferTimeout time.Duration `yaml:"minOutputBufferTimeout"`
	FlushInterval          time.Duration `yaml:"flushInterval"`

	// TLS config
	TLSCert             string `yaml:"tlsCert"`
	TLSKey              string `yaml:"tlsKey"`
	TLSClientAuthPolicy string `yaml:"tlsClientAuthPolicy"`
	TLSRootCAFile       string `yaml:"tlsRootCaFile"`
	TLSRequired         int    `yaml:"tlsRequired"`
	TLSMinVersion       uint16 `yaml:"tlsMinVersion"`
}

func NewConfig() *Config {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Config{
		NodeID: defaultID,

		TCPAddress:   "0.0.0.0:9798",
		HTTPAddress:  "0.0.0.0:9799",
		HTTPSAddress: "0.0.0.0:9800",

		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,

		MsgTimeout:       60 * time.Second,
		MaxMsgTimeout:    15 * time.Minute,
		MaxMsgSize:       1024 * 1024,
		MaxBodySize:      5 * 1024 * 1024,
		MaxReqTimeout:    1 * time.Hour,
		HeartbeatTimeout: 60 * time.Second,

		MaxHeartbeatInterval:   60 * time.Second,
		MaxOutputBufferSize:    64 * 1024,
		MaxOutputBufferTimeout: 30 * time.Second,
		MinOutputBufferTimeout: 25 * time.Millisecond,
		FlushInterval:          250 * time.Millisecond,

		TLSMinVersion: tls.VersionTLS10,
	}
}
