package retryproxy

import (
	"fmt"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
	"sync"
	"time"
)

const SLEEP_DURATION_INITIAL uint64 = 0
const SLEEP_DURATION_MAX uint64 = 60

type EtcdClientRetryProxy struct {
	keysAPI client.KeysAPI
	waitSeconds      uint64
	waitSecondsMax   uint64
	waitSecondsMutex sync.RWMutex
	resultChannel    chan result
}

type result int

const (
	SUCCESS result = iota
	ERROR
)

func NewEtcdClientRetryProxy(c client.Client) *EtcdClientRetryProxy {
	ans := new(EtcdClientRetryProxy)
	ans.keysAPI = client.NewKeysAPI(c)
	ans.waitSecondsMax = SLEEP_DURATION_MAX      // TODO: make this configurable?
	ans.setSleepDuration(SLEEP_DURATION_INITIAL) // on fail, start waiting 1 second
	ans.resultChannel = make(chan result, 1024)

	go ans.sleepManager()

	return ans
}

func (ecrp *EtcdClientRetryProxy) sleepManager() {
	for {
		switch <-ecrp.resultChannel {
		case SUCCESS:
			ecrp.decreaseSleep()
		case ERROR:
			ecrp.increaseSleep()
		}
	}
}

func (ecrp *EtcdClientRetryProxy) Retry(fn func() (*client.Response, error)) (*client.Response, error) {
	var ans *client.Response
	var err error

	// if fn() fails due to ErrClusterUnavailable, retry
	for ans, err = fn(); err != nil && err.Error() == client.ErrClusterUnavailable.Error(); ans, err = fn() {
		fmt.Println(err)
		fmt.Printf("sleeping for %+v second(s)...\n", ecrp.sleepDuration())
		time.Sleep(time.Duration(ecrp.sleepDuration()) * time.Second)
		ecrp.resultChannel <- ERROR
	}

	// no errors, so reset the sleep duration for the next call
	ecrp.resultChannel <- SUCCESS

	return ans, err
}

func (ecrp *EtcdClientRetryProxy) sleepDuration() uint64 {
	ecrp.waitSecondsMutex.RLock()
	defer ecrp.waitSecondsMutex.RUnlock()
	return ecrp.waitSeconds
}

func (ecrp *EtcdClientRetryProxy) setSleepDuration(s uint64) {
	ecrp.waitSecondsMutex.Lock()
	defer ecrp.waitSecondsMutex.Unlock()
	ecrp.waitSeconds = s
}

func (ecrp *EtcdClientRetryProxy) decreaseSleep() {
	// go prevents buffer underrun, so
  s := ecrp.sleepDuration()
  if s > SLEEP_DURATION_INITIAL {
	  ecrp.setSleepDuration(s >> 1)
  }
}

func (ecrp *EtcdClientRetryProxy) increaseSleep() {
	var val uint64
	switch old := ecrp.sleepDuration(); old {
	case SLEEP_DURATION_INITIAL:
		val = SLEEP_DURATION_INITIAL + 1
	default:
		val = old << 1
		if val > ecrp.waitSecondsMax {
			val = ecrp.waitSecondsMax
		}
	}

	ecrp.setSleepDuration(val)
}

// Satisfy the client.KeysAPI interface

func (ecrp *EtcdClientRetryProxy) Get(ctx context.Context, key string, opts *client.GetOptions) (*client.Response, error) {
	return ecrp.Retry(func() (*client.Response, error) { return ecrp.keysAPI.Get(ctx, key, opts) })
}

func (ecrp *EtcdClientRetryProxy) Set(ctx context.Context, key, value string, opts *client.SetOptions) (*client.Response, error) {
	return ecrp.Retry(func() (*client.Response, error) { return ecrp.keysAPI.Set(ctx, key, value, opts) })
}

func (ecrp *EtcdClientRetryProxy) Delete(ctx context.Context, key string, opts *client.DeleteOptions) (*client.Response, error) {
	return ecrp.Retry(func() (*client.Response, error) { return ecrp.keysAPI.Delete(ctx, key, opts) })
}

func (ecrp *EtcdClientRetryProxy) Create(ctx context.Context, key, value string) (*client.Response, error) {
	return ecrp.Retry(func() (*client.Response, error) { return ecrp.keysAPI.Create(ctx, key, value) })
}

func (ecrp *EtcdClientRetryProxy) CreateInOrder(ctx context.Context, dir, value string, opts *client.CreateInOrderOptions) (*client.Response, error) {
	return ecrp.Retry(func() (*client.Response, error) { return ecrp.keysAPI.CreateInOrder(ctx, dir, value, opts) })
}

func (ecrp *EtcdClientRetryProxy) Update(ctx context.Context, key, value string) (*client.Response, error) {
	return ecrp.Retry(func() (*client.Response, error) { return ecrp.keysAPI.Update(ctx, key, value) })
}

func (ecrp *EtcdClientRetryProxy) Watcher(key string, opts *client.WatcherOptions) client.Watcher {
	return ecrp.NewWatcherProxy(key, opts)
}

type watcherProxy struct {
	watcher          client.Watcher
	ecrp             *EtcdClientRetryProxy
	max_wait_seconds uint64
	watcherSleep     uint64
}

func (ecrp *EtcdClientRetryProxy) NewWatcherProxy(key string, opts *client.WatcherOptions) *watcherProxy {
	ans := new(watcherProxy)
	ans.watcher = ecrp.keysAPI.Watcher(key, opts)
	ans.ecrp = ecrp
	ans.max_wait_seconds = SLEEP_DURATION_MAX // TODO: customizable?
	ans.watcherSleep = SLEEP_DURATION_INITIAL
	return ans
}

func (wp *watcherProxy) Next(ctx context.Context) (*client.Response, error) {
	if wp.watcherSleep > SLEEP_DURATION_INITIAL {
		time.Sleep(time.Duration(wp.watcherSleep) * time.Second)
		wp.watcherSleep = wp.watcherSleep << 1 // sleep twice as long next time
		if wp.watcherSleep > wp.max_wait_seconds {
			wp.watcherSleep = wp.max_wait_seconds
		}
		return wp.watcher.Next(ctx)
	}

	ans, err := wp.watcher.Next(ctx)
	if err != nil {
		wp.watcherSleep = 1
	}

	return ans, err
}
