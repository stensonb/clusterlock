package clusterlock

import (
	"fmt"
	"os"
	"testing"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/coreos/etcd/client"
	"github.com/stensonb/clusterlock/retryproxy"
)

const LockTTL = 10 * time.Second

// NOTE: this test depends on an etcd cluster available
// on 127.0.0.1:2379 (the default port)
// TODO: remove the dependence on etcd for testing
func TestLockManager(t *testing.T) {
	t.Parallel()

	cfg := client.Config{
		Endpoints: []string{"http://127.0.0.1:2379"},
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, _ := client.New(cfg)

	ecrpErrorChan := make(chan error)
	ecrp := retryproxy.NewEtcdClientRetryProxy(c, ecrpErrorChan, 1, 60)
	path := "locktest"
	hn, _ := os.Hostname()
	path += hn
	path += uuid.New()
	ttl := LockTTL

	lm := NewLockManager(ecrp, path, ttl)
	defer lm.Shutdown()

	closeit := time.After(10 * time.Second)

	for {
		select {
		case <-closeit:
			return
		case <-time.After(time.Second):
			fmt.Printf("Have Lock: %t\n", lm.HaveLock())
		}
	}

}
