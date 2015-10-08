package retryproxyagain

import (
	"fmt"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
	"time"
)

type EtcdClientRetryProxy struct {
	keysAPI client.KeysAPI
}

func NewEtcdClientRetryProxy(c client.Client) *EtcdClientRetryProxy {
	ans := new(EtcdClientRetryProxy)
	ans.keysAPI = client.NewKeysAPI(c)

	return ans
}

func (ecrp *EtcdClientRetryProxy) Retry(fn func() (*client.Response, error)) (*client.Response, error) {
	var ans *client.Response
	var err error

	// if fn() fails due to ErrClusterUnavailable, retry
	for ans, err = fn(); err != nil && err.Error() == client.ErrClusterUnavailable.Error(); ans, err = fn() {
		fmt.Println(err)
		fmt.Printf("sleeping for %+v second(s)...\n", 1)
		time.Sleep(time.Duration(1) * time.Second)
	}

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
	watcher          client.Watcher
	ecrp             *EtcdClientRetryProxy
}

func (ecrp *EtcdClientRetryProxy) NewWatcherProxy(key string, opts *client.WatcherOptions) *watcherProxy {
	ans := new(watcherProxy)
	ans.watcher = ecrp.keysAPI.Watcher(key, opts)
	ans.ecrp = ecrp
	return ans
}

func (wp *watcherProxy) Next(ctx context.Context) (*client.Response, error) {
	ans, err := wp.watcher.Next(ctx)

	return ans, err
}
