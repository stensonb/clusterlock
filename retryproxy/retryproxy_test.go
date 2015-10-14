package retryproxy

import (
	"testing"
	"time"

	"github.com/coreos/etcd/client"
)

func TestEtcdClientRetryProxy(t *testing.T) {
	t.Parallel()
	cfg := client.Config{
		Endpoints: []string{"http://127.0.0.1:2379"},
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, _ := client.New(cfg)

	//  _ :=
	NewEtcdClientRetryProxy(c, nil, 1, 60)
}
