package broker

import (
	"log"
	"os"
)

// Logger is the minimal logging surface used by the broker. It mirrors the
// standard library *log.Logger so tests can swap in a buffer.
type Logger interface {
	Printf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

func defaultLogger() Logger {
	return log.New(os.Stdout, "[mqtt] ", log.LstdFlags|log.Lmicroseconds)
}
