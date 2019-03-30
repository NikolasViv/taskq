package msgqueue_test

import (
	"testing"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/ironmq"

	iron_config "github.com/iron-io/iron_go3/config"
)

func ironmqManager() msgqueue.Manager {
	settings := iron_config.Config("iron_mq")
	return ironmq.NewManager(&settings)
}

func TestIronmqProcessor(t *testing.T) {
	testProcessor(t, ironmqManager(), &msgqueue.QueueOptions{
		Name: queueName("ironmq-processor"),
	})
}

func TestIronmqFallback(t *testing.T) {
	testFallback(t, ironmqManager(), &msgqueue.QueueOptions{
		Name: queueName("ironmq-fallback"),
	})
}

func TestIronmqDelay(t *testing.T) {
	testDelay(t, ironmqManager(), &msgqueue.QueueOptions{
		Name: queueName("ironmq-delay"),
	})
}

func TestIronmqRetry(t *testing.T) {
	testRetry(t, ironmqManager(), &msgqueue.QueueOptions{
		Name: queueName("ironmq-retry"),
	})
}

func TestIronmqNamedMessage(t *testing.T) {
	testNamedMessage(t, ironmqManager(), &msgqueue.QueueOptions{
		Name: queueName("ironmq-named-message"),
	})
}

func TestIronmqCallOnce(t *testing.T) {
	testCallOnce(t, ironmqManager(), &msgqueue.QueueOptions{
		Name: queueName("ironmq-call-once"),
	})
}

func TestIronmqLen(t *testing.T) {
	testLen(t, ironmqManager(), &msgqueue.QueueOptions{
		Name: queueName("ironmq-len"),
	})
}

func TestIronmqRateLimit(t *testing.T) {
	testRateLimit(t, ironmqManager(), &msgqueue.QueueOptions{
		Name: queueName("ironmq-rate-limit"),
	})
}

func TestIronmqErrorDelay(t *testing.T) {
	testErrorDelay(t, ironmqManager(), &msgqueue.QueueOptions{
		Name: queueName("ironmq-delayer"),
	})
}

func TestIronmqWorkerLimit(t *testing.T) {
	testWorkerLimit(t, ironmqManager(), &msgqueue.QueueOptions{
		Name: queueName("worker-limit"),
	})
}

func TestIronmqInvalidCredentials(t *testing.T) {
	settings := &iron_config.Settings{
		ProjectId: "123",
	}
	manager := ironmq.NewManager(settings)
	testInvalidCredentials(t, manager, &msgqueue.QueueOptions{
		Name: queueName("invalid-credentials"),
	})
}
