// TODO: remove * on interfaces in struct defs
// - shutdown options for goroutines

// distributed lock manager, using etcd
// provides a cluster-level "lock" which
// can be used when a cluster-level singleton
// is required.
// by default, loss of communication with etcd
// causes the lock manager to release the lock

package main

import (
	"code.google.com/p/go-uuid/uuid"
	"errors"
	"fmt"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
//	"github.com/stensonb/lockplay/retryproxy"
retryproxy "github.com/stensonb/lockplay/retryproxyagain"
	"os"
	//"runtime"
	"sync"
	"time"
)

const LOCK_TTL = 10 * time.Second

func main() {
	cfg := client.Config{
		Endpoints: []string{"http://127.0.0.1:2379"},
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, _ := client.New(cfg)

	ecrp := retryproxy.NewEtcdClientRetryProxy(c) //client.NewKeysAPI(c)

	path := "/foo/lock"
	lm := NewLockManager(ecrp, path)

	for {
		fmt.Printf("Have Lock: %t\n", lm.HaveLock())
		time.Sleep(time.Second)
	}
}

type LockManager struct {
	ecrp                   *retryproxy.EtcdClientRetryProxy //client.KeysAPI
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

func NewLockManager(ecrp *retryproxy.EtcdClientRetryProxy, path string) *LockManager {
	// initialization
	ans := new(LockManager)
	ans.ecrp = ecrp
	ans.path = path
	ans.setLockStatus(false)
	ans.id, _ = os.Hostname()
	ans.id += uuid.New()
	ans.lockRemoved = make(chan bool, 1)
	ans.setCurIndex(uint64(0))

	fmt.Println("Lock ID:", ans.id)

  //go ans.etcdWatcher()
	go ans.watchLock()
	go ans.lockGetter()

	return ans
}

// sole purpose is to watch etcd
// when it becomes in accessible, report it
func (lm *LockManager) etcdWatcher() {
	/*
	for {
	  w := (*(lm.ecrp)).Watcher(lm.path, nil)

  	fmt.Println("monitoring etcd:", lm.path)
  	for {
  		resp, err := w.Next(context.Background())
		}
	}
	*/
}

func (lm *LockManager) HaveLock() bool {
	lm.lockStatusMutex.RLock()
	defer lm.lockStatusMutex.RUnlock()
	return lm.lockStatus
}

func (lm *LockManager) Shutdown() {
	// TODO: exit the goroutines, remove the lock
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
