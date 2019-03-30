package msgqueue_test

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/azsqs"
)

var accountId string

func init() {
	accountId = os.Getenv("AWS_ACCOUNT_ID")
}

func awsSQS() *sqs.SQS {
	return sqs.New(session.New())
}

func azsqsManager() msgqueue.Manager {
	return azsqs.NewManager(awsSQS(), accountId)
}

func TestSQSProcessor(t *testing.T) {
	testProcessor(t, azsqsManager(), &msgqueue.QueueOptions{
		Name: queueName("processor"),
	})
}

func TestSQSFallback(t *testing.T) {
	testFallback(t, azsqsManager(), &msgqueue.QueueOptions{
		Name: queueName("fallback"),
	})
}

func TestSQSDelay(t *testing.T) {
	testDelay(t, azsqsManager(), &msgqueue.QueueOptions{
		Name: queueName("delay"),
	})
}

func TestSQSRetry(t *testing.T) {
	testRetry(t, azsqsManager(), &msgqueue.QueueOptions{
		Name: queueName("retry"),
	})
}

func TestSQSNamedMessage(t *testing.T) {
	testNamedMessage(t, azsqsManager(), &msgqueue.QueueOptions{
		Name: queueName("named-message"),
	})
}

func TestSQSCallOnce(t *testing.T) {
	testCallOnce(t, azsqsManager(), &msgqueue.QueueOptions{
		Name: queueName("call-once"),
	})
}

func TestSQSLen(t *testing.T) {
	testLen(t, azsqsManager(), &msgqueue.QueueOptions{
		Name: queueName("queue-len"),
	})
}

func TestSQSRateLimit(t *testing.T) {
	testRateLimit(t, azsqsManager(), &msgqueue.QueueOptions{
		Name: queueName("rate-limit"),
	})
}

func TestSQSErrorDelay(t *testing.T) {
	testErrorDelay(t, azsqsManager(), &msgqueue.QueueOptions{
		Name: queueName("delayer"),
	})
}

func TestSQSWorkerLimit(t *testing.T) {
	testWorkerLimit(t, azsqsManager(), &msgqueue.QueueOptions{
		Name: queueName("worker-limit"),
	})
}

func TestSQSInvalidCredentials(t *testing.T) {
	man := azsqs.NewManager(awsSQS(), "123")
	testInvalidCredentials(t, man, &msgqueue.QueueOptions{
		Name: queueName("invalid-credentials"),
	})
}

func TestSQSBatchProcessorSmallMessage(t *testing.T) {
	testBatchProcessor(t, azsqsManager(), &msgqueue.QueueOptions{
		Name: queueName("batch-processor-small-message"),
	}, 100)
}

func TestSQSBatchProcessorLarge(t *testing.T) {
	testBatchProcessor(t, azsqsManager(), &msgqueue.QueueOptions{
		Name: queueName("batch-processor-large-message"),
	}, 64000)
}
