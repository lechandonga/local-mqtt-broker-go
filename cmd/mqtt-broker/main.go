// Command mqtt-broker runs the local MQTT 3.1.1 broker.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lechandonga/local-mqtt-broker-go/internal/broker"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:1883", "TCP listen address")
	flag.Parse()

	logger := log.New(os.Stdout, "mqtt ", log.LstdFlags|log.Lmicroseconds)
	b := broker.New(broker.Config{Addr: *addr, Logger: logger})
	if err := b.Start(); err != nil {
		log.Fatalf("listen: %v", err)
	}
	logger.Printf("MQTT 3.1.1 broker listening on %s", b.Addr())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Printf("shutting down")
	_ = b.Close()
}
