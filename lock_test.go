package lock

import (
	"testing"
	//"fmt"
	//"github.com/coreos/etcd/client"
	//"github.com/stensonb/lockplay/retryproxy"
	"time"
)

const LOCK_TTL = 10 * time.Second

func TestLockManager(t *testing.T) {
	/*
		cfg := client.Config{
			Endpoints: []string{"http://127.0.0.1:2379"},
			Transport: client.DefaultTransport,
			// set timeout per request to fail fast when the target endpoint is unavailable
			HeaderTimeoutPerRequest: time.Second,
		}
		c, _ := client.New(cfg)

		ecrpErrorChan := make(chan error)
		ecrp := retryproxy.NewEtcdClientRetryProxy(c, ecrpErrorChan)
		path := "/foo/lock"
		ttl := LOCK_TTL

		lm := NewLockManager(ecrp, ecrpErrorChan, path, ttl)
		defer lm.Shutdown()

		closeit := time.After(1 * time.Second)

		for {
			select {
			case <-closeit:
				return
			case <-time.After(time.Second):
				fmt.Printf("Have Lock: %t\n", lm.HaveLock())
			}
		}
	*/
}
