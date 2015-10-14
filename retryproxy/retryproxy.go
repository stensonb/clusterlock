package retryproxy

import (
	"fmt"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
	"github.com/stensonb/clusterlock/sleepManager"
)

type EtcdClientRetryProxy struct {
	keysAPI        client.KeysAPI
	sm             *sleepManager.SleepManager
	errorChan      chan error
	quitChannel    chan interface{}
	ourQuitChannel bool
}

// if you don't want to be notified when the retry proxy needs to retry, pass ec=nil
func NewEtcdClientRetryProxy(c client.Client, ec chan error, min uint, max uint) *EtcdClientRetryProxy {
	ans := new(EtcdClientRetryProxy)
	ans.keysAPI = client.NewKeysAPI(c)
	ans.sm = sleepManager.NewSleepManager(min, max)

	// the user doesn't care about retries,
	// so create an error channel and create
	// a goroutine to drain the channel
	if ec == nil {
		r := make(chan error)
		ans.errorChan = r
		ans.ourQuitChannel = true

		// drain the channel, as needed
		go func() {
			for {
				select {
				case <-r:
				case <-ans.quitChannel:
					return
				}
			}
		}()

	} else {
		ans.errorChan = ec
	}

	ans.quitChannel = make(chan interface{})

	return ans
}

func (ecrp *EtcdClientRetryProxy) Shutdown() {
	ecrp.sm.Shutdown()
	if ecrp.ourQuitChannel {
		close(ecrp.quitChannel)
	}
}

func (ecrp *EtcdClientRetryProxy) Retry(fn func() (*client.Response, error)) (*client.Response, error) {
	var ans *client.Response
	var err error

	// if fn() fails due to ErrClusterUnavailable, retry
	for ans, err = fn(); err != nil && err.Error() == client.ErrClusterUnavailable.Error(); ans, err = fn() {
		go ecrp.sm.Error() // non-blocking
		ecrp.errorChan <- err
		fmt.Println(err)
		ecrp.sm.Sleep()
	}

	go ecrp.sm.Success() // non-blocking

	return ans, err
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
	watcher client.Watcher
	ecrp    *EtcdClientRetryProxy
}

func (ecrp *EtcdClientRetryProxy) NewWatcherProxy(key string, opts *client.WatcherOptions) *watcherProxy {
	ans := new(watcherProxy)
	ans.watcher = ecrp.keysAPI.Watcher(key, opts)
	ans.ecrp = ecrp
	return ans
}

func (wp *watcherProxy) Next(ctx context.Context) (*client.Response, error) {
	return wp.ecrp.Retry(func() (*client.Response, error) { return wp.watcher.Next(ctx) })
}
