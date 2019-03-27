package msgqueue

import (
	"log"
	"os"

	"github.com/go-msgqueue/msgqueue/internal"
)

func init() {
	SetLogger(log.New(os.Stderr, "msgqueue: ", log.LstdFlags))
}

func SetLogger(logger *log.Logger) {
	internal.Logger = logger
}
