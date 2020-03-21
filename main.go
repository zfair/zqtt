package main

import (
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/judwhite/go-svc/svc"
	"github.com/zfair/zqtt/broker"
	"github.com/zfair/zqtt/internal/config"
	"gopkg.in/yaml.v2"
)

type program struct {
	once   sync.Once
	server *broker.Server
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		log.Fatalf("%s", err)
	}
}
func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

func (p *program) Start() error {
	cfg := config.NewConfig()

	flagSet := brokerFlagSet()
	flagSet.Parse(os.Args[1:])

	rand.Seed(time.Now().UTC().UnixNano())

	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		buf, err := ioutil.ReadFile(configFile)
		if err != nil {
			log.Fatalf("failed to load config file %s - %s", configFile, err)
			os.Exit(1)
		}
		err = yaml.Unmarshal(buf, &cfg)
		if err != nil {
			log.Fatalf("failed to load config file %s - %s", configFile, err)
			os.Exit(1)
		}
	}

	server, err := broker.NewServer(cfg)
	if err != nil {
		log.Fatalf("failed to instantiate nsqd - %s", err)
	}
	p.server = server

	go func() {
		err := p.server.Main()
		if err != nil {
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	p.once.Do(func() {
		p.server.Exit()
	})
	return nil
}

func brokerFlagSet() *flag.FlagSet {
	flagSet := flag.NewFlagSet("broker", flag.ExitOnError)
	// basic options
	flagSet.Bool("version", false, "print version string")
	flagSet.String("config", "", "path to config file")

	return flagSet
}
