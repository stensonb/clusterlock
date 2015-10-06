// TODO: remove * on interfaces in struct defs
// - shutdown options for goroutines

package main

import (
	"code.google.com/p/go-uuid/uuid"
	"errors"
	"fmt"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
	"os"
	"runtime"
	"sync"
	"time"
)

const LOCK_TTL = 10 * time.Second

type Breaker bool

const CLOSED = true // allow traffic to flow
const OPEN = false  // prevent traffic from flowing

type EtcdClientRetryProxy struct {
	keysAPI          client.KeysAPI
	breaker          Breaker
	breakerMutex     sync.RWMutex
	breakerChan      chan bool
	wait_seconds     uint64
	max_wait_seconds uint64
}

func NewEtcdClientRetryProxy(c client.Client) *EtcdClientRetryProxy {
	ans := new(EtcdClientRetryProxy)
	ans.keysAPI = client.NewKeysAPI(c)
	ans.breaker = CLOSED
	ans.breakerChan = make(chan bool, 1)
	ans.wait_seconds = 1              // on fail, start waiting 1 second
	ans.max_wait_seconds = uint64(60) // TODO: make this configurable?
	return ans
}

func (ecrp *EtcdClientRetryProxy) EtcdAvailable() Breaker {
	ecrp.breakerMutex.RLock()
	defer ecrp.breakerMutex.RUnlock()
	return ecrp.breaker
}

func (ecrp *EtcdClientRetryProxy) setBreaker(b Breaker) {
	ecrp.breakerMutex.Lock()
	defer ecrp.breakerMutex.Unlock()
	ecrp.breaker = b
}

func (ecrp *EtcdClientRetryProxy) Retry(fn func()(*client.Response, error)) (*client.Response, error) {
	var ans *client.Response
	var err error
	var sleep uint64 = 1

  for ans, err = fn(); err != nil && err.Error() == client.ErrClusterUnavailable.Error(); ans, err = fn() {
		fmt.Printf("sleeping for %+v second(s)...\n", sleep)
		time.Sleep(time.Duration(sleep) * time.Second)
		fmt.Println("awake!")
		sleep = sleep << 1 //sleep twice as long next time
		if sleep > ecrp.max_wait_seconds {
			sleep = ecrp.max_wait_seconds
		}
	}

  return ans, err
}


func (ecrp *EtcdClientRetryProxy) Get(ctx context.Context, key string, opts *client.GetOptions) (*client.Response, error) {
  return ecrp.Retry(func() (*client.Response, error) { return ecrp.keysAPI.Get(ctx, key, opts) } )
}

func (ecrp *EtcdClientRetryProxy) Set(ctx context.Context, key, value string, opts *client.SetOptions) (*client.Response, error) {
  return ecrp.Retry(func() (*client.Response, error) { return ecrp.keysAPI.Set(ctx, key, value, opts) } )
}

func (ecrp *EtcdClientRetryProxy) Delete(ctx context.Context, key string, opts *client.DeleteOptions) (*client.Response, error) {
  return ecrp.Retry(func() (*client.Response, error) { return ecrp.keysAPI.Delete(ctx, key, opts) } )
}

func (ecrp *EtcdClientRetryProxy) Create(ctx context.Context, key, value string) (*client.Response, error) {
  return ecrp.Retry(func() (*client.Response, error) { return ecrp.keysAPI.Create(ctx, key, value) } )
}

func (ecrp *EtcdClientRetryProxy) CreateInOrder(ctx context.Context, dir, value string, opts *client.CreateInOrderOptions) (*client.Response, error) {
  return ecrp.Retry(func() (*client.Response, error) { return ecrp.keysAPI.CreateInOrder(ctx, dir, value, opts) } )
}

func (ecrp *EtcdClientRetryProxy) Update(ctx context.Context, key, value string) (*client.Response, error) {
  return ecrp.Retry(func() (*client.Response, error) { return ecrp.keysAPI.Update(ctx, key, value) } )
}

func (ecrp *EtcdClientRetryProxy) Watcher(key string, opts *client.WatcherOptions) client.Watcher {
	return ecrp.NewWatcherProxy(key, opts)
}

type WatcherProxy struct {
	watcher          client.Watcher
	ecrp             *EtcdClientRetryProxy
	max_wait_seconds uint64
	watcherSleep uint64
}

func (ecrp *EtcdClientRetryProxy) NewWatcherProxy(key string, opts *client.WatcherOptions) *WatcherProxy {
	ans := new(WatcherProxy)
	ans.watcher = ecrp.keysAPI.Watcher(key, opts)
	ans.ecrp = ecrp
	ans.max_wait_seconds = 60 // TODO: customizable?
	ans.watcherSleep = 0
	return ans
}

func (wp *WatcherProxy) Next(ctx context.Context) (*client.Response, error) {
	if wp.watcherSleep > 0 {
		time.Sleep(time.Duration(wp.watcherSleep) * time.Second)
		wp.watcherSleep = wp.watcherSleep << 1  // sleep twice as long next time
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

func main() {
	cfg := client.Config{
		Endpoints: []string{"http://127.0.0.1:2379"},
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, _ := client.New(cfg)

	ecrp := NewEtcdClientRetryProxy(c) //client.NewKeysAPI(c)

	path := "/foo/lock"
	lm := NewLockManager(ecrp, path)

	for {
		fmt.Printf("Have Lock: %t\n", lm.HaveLock())
		time.Sleep(time.Second)
	}

}

type LockManager struct {
	ecrp                   *EtcdClientRetryProxy //client.KeysAPI
	path                   string
	id                     string
	lockStatusMutex        sync.RWMutex
	lockStatus             bool
	lockRemoved            chan bool
	curIndex               uint64 // modificationIndex of etcd value
	curIndexMutex          sync.RWMutex
	lockKeeperRunningVal   bool
	lockKeeperRunningMutex sync.RWMutex
}

func NewLockManager(ecrp *EtcdClientRetryProxy, path string) *LockManager {
	ans := new(LockManager)
	ans.ecrp = ecrp
	ans.path = path
	ans.setLockStatus(false)
	ans.id, _ = os.Hostname()
	ans.id += uuid.New()
	ans.lockRemoved = make(chan bool, 1)
	ans.setCurIndex(uint64(0))

	fmt.Println("Lock ID:", ans.id)

	go ans.watchLock()
	go ans.lockGetter()

	return ans
}

func (lm *LockManager) HaveLock() bool {
	lm.lockStatusMutex.RLock()
	defer lm.lockStatusMutex.RUnlock()
	return lm.lockStatus
}

func (lm *LockManager) Shutdown() {
	buf := make([]byte, 1<<20)
	runtime.Stack(buf, true)
	fmt.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf)
}

func (lm *LockManager) getCurIndex() uint64 {
	lm.curIndexMutex.RLock()
	defer lm.curIndexMutex.RUnlock()
	return lm.curIndex
}

func (lm *LockManager) setCurIndex(val uint64) {
	lm.curIndexMutex.Lock()
	defer lm.curIndexMutex.Unlock()
	lm.curIndex = val
}

func (lm *LockManager) lockGetter() {
	// always work towards getting the lock
	for {
		// read lock
		lock, err := lm.readLock()
		if err != nil {
			//TODO: if the error is that the key doesn't exist
			// if it doesn't exist -- attempt to set it
			fmt.Println("lock missing...")
			err2 := lm.createLock()
			if err2 != nil {
				fmt.Printf("failed to createLock: %+v\n", err2)
			}
			// on success or failure, start the loop again
			continue
		}

		// if we have it, keep it updated
		if lock == lm.id {
			go lm.lockKeeper()
		}

		// if another instance has it -- block until expired/deleted
		fmt.Println("waiting for lock removal...")
		<-lm.lockRemoved
	}
}

func (lm *LockManager) lockKeeperRunning() bool {
	lm.lockKeeperRunningMutex.RLock()
	defer lm.lockKeeperRunningMutex.RUnlock()
	return lm.lockKeeperRunningVal
}

func (lm *LockManager) setLockKeeperRunning(val bool) {
	lm.lockKeeperRunningMutex.Lock()
	defer lm.lockKeeperRunningMutex.Unlock()
	lm.lockKeeperRunningVal = val
}

func (lm *LockManager) lockKeeper() {
	if lm.lockKeeperRunning() {
		// there's already a go routine
		// running to refresh the lock
		return
	}

	// when this function exists,
	// be sure to release the running mutex
	lm.setLockKeeperRunning(true)
	defer lm.setLockKeeperRunning(false)

	var err error = nil

	for err == nil && lm.HaveLock() {
		err = lm.updateTTL()

		// only sleep if we successfully
		// updated the ttl...otherwise, quit quickly.
		if err == nil {
			lm.ttlSleep()
		}
	}
}

func (lm *LockManager) ttlSleep() {
	// some delay -- half of the TTL time
	if lm.HaveLock() {
		time.Sleep((LOCK_TTL / 2))
	}
}

func (lm *LockManager) readLock() (string, error) {
	// read lock entry in etcd
	opts := client.GetOptions{Quorum: true}
	resp, err := (*(lm.ecrp)).Get(context.Background(), lm.path, &opts)

	// if lock missing, return err
	if err != nil {
		return "", err
	}

	return resp.Node.Value, nil
}

func (lm *LockManager) updateTTL() error {
	// if we don't have the lock locally, return
	if !lm.HaveLock() {
		return errors.New("not our lock to manage...")
	}

	fmt.Println("updating TTL...")
	// set the ttl, ensuring the lock already exists
	// and that verify PrevIndex is what we expect
	opts := client.SetOptions{TTL: LOCK_TTL, PrevExist: client.PrevExist, PrevIndex: lm.getCurIndex()}
	resp, err := (*(lm.ecrp)).Set(context.Background(), lm.path, lm.id, &opts)
	if err != nil {
		return err
	}
	lm.setCurIndex(resp.Index)
	return nil
}

func (lm *LockManager) createLock() error {
	// write lm.id to lm.path with LOCK_TTL in etcd,
	// ensuring there is no previous value
	opts := client.SetOptions{TTL: LOCK_TTL, PrevExist: client.PrevNoExist}
	resp, err := (*(lm.ecrp)).Set(context.Background(), lm.path, lm.id, &opts)
	if err != nil {
		return err
	}

	lm.setCurIndex(resp.Index)
	lm.setLockStatus(true)
	return nil
}

func (lm *LockManager) setLockStatus(val bool) {
	lm.lockStatusMutex.Lock()
	defer lm.lockStatusMutex.Unlock()
	lm.lockStatus = val
}

func (lm *LockManager) watchLock() {
	// start a watcher on the lock
	w := (*(lm.ecrp)).Watcher(lm.path, nil)

	fmt.Println("watching:", lm.path)
	for {
		resp, err := w.Next(context.Background())
		if err != nil {
			// on etcd failure, w.Next will continue to try
			// and read the next watch message -- and when
			// etcd becomes available, this loop will continue
			// to operate correctly
			fmt.Printf("error: %+v\n", err)
			// give up the lock (since we don't know the state of etcd)
			if lm.HaveLock() {
				lm.setLockStatus(false)
				lm.lockRemoved <- true
			}
			// continue the loop
			continue
		}
		lm.handleWatchResult(resp)
	}
}

func (lm *LockManager) handleWatchResult(resp *client.Response) {
	switch resp.Action {
	case "expire", "delete":
		lm.setLockStatus(false) // we no longer have the lock (if we ever did)
		fmt.Println("expired or deleted")
		lm.lockRemoved <- true // announce the lock removal
	}
}
