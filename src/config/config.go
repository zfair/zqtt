package config

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Config for internal/customizable configurations.
type Config struct {
	// Basic options.
	NodeID int64 `yaml:"nodeID"`
	Logger *zap.Logger

	TCPAddress               string        `yaml:"tcpAddress"`
	HTTPAddress              string        `yaml:"httpAddress"`
	HTTPSAddress             string        `yaml:"httpsAddress"`
	HTTPClientConnectTimeout time.Duration `yaml:"httpClientConnectTimeout"`
	HTTPClientRequestTimeout time.Duration `yaml:"httpClientRequestTimeout"`

	// Message and command options.
	MsgTimeout       time.Duration `yaml:"msgTimeout"`
	MaxMsgTimeout    time.Duration `yaml:"maxMsgTimeout"`
	MaxMsgSize       int64         `yaml:"maxMsgSize"`
	MaxBodySize      int64         `yaml:"maxBodySize"`
	MaxReqTimeout    time.Duration `yaml:"maxReqTimeout"`
	HeartbeatTimeout time.Duration `yaml:"heartbeatTimeout"`

	// Customizable configuration options.
	MaxHeartbeatInterval   time.Duration `yaml:"maxHeartbeatInterval"`
	MaxOutputBufferSize    int64         `yaml:"maxOutputBufferSize"`
	MaxOutputBufferTimeout time.Duration `yaml:"maxOutputBufferTimeout"`
	MinOutputBufferTimeout time.Duration `yaml:"minOutputBufferTimeout"`
	FlushInterval          time.Duration `yaml:"flushInterval"`

	// TLS config.
	TLSCert             string `yaml:"tlsCert"`
	TLSKey              string `yaml:"tlsKey"`
	TLSClientAuthPolicy string `yaml:"tlsClientAuthPolicy"`
	TLSRootCAFile       string `yaml:"tlsRootCaFile"`
	TLSRequired         int    `yaml:"tlsRequired"`
	TLSMinVersion       uint16 `yaml:"tlsMinVersion"`

	// Storage config.
	MStorage *ProviderInfo `yaml:"mstorage"`
	SStorage *ProviderInfo `yaml:"sstorage"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	_, _ = io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Config{
		NodeID: defaultID,

		TCPAddress:   "127.0.0.1:9798",
		HTTPAddress:  "127.0.0.1:9799",
		HTTPSAddress: "127.0.0.1:9800",

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

// Provider is the config provider interface.
type Provider interface {
	Name() string
	Configure(ctx context.Context, config map[string]interface{}) error
}

// ProviderInfo is the info of a config provider.
type ProviderInfo struct {
	Provider string                 `yaml:"provider"`
	Config   map[string]interface{} `yaml:"config,omitempty"`
}

// LoadProvider Find And Load Provider Config Into Provider
func LoadProvider(ctx context.Context, info *ProviderInfo, providers ...Provider) (interface{}, error) {
	providerName := info.Provider
	var provider Provider
	for _, p := range providers {
		// find the match provider
		if p.Name() == providerName {
			provider = p
			break
		}
	}
	if provider == nil {
		return nil, errors.Errorf("Provider %s Not Found", providerName)
	}
	err := provider.Configure(ctx, info.Config)
	return provider, err
}
