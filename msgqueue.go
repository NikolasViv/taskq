package taskq

import (
	"log"
	"os"

	"github.com/vmihailenco/taskq/internal"
)

func init() {
	SetLogger(log.New(os.Stderr, "taskq: ", log.LstdFlags))
}

func SetLogger(logger *log.Logger) {
	internal.Logger = logger
}
