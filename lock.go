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

func main() {
	cfg := client.Config{
		Endpoints: []string{"http://127.0.0.1:2379"},
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, _ := client.New(cfg)
	kapi := client.NewKeysAPI(c)

	path := "/foo/lock"
	lm := NewLockManager(&kapi, path)

	for {
		fmt.Printf("Have Lock: %t\n", lm.HaveLock())
		time.Sleep(time.Second)
	}
}

type LockManager struct {
	kapi            *client.KeysAPI
	path            string
	id              string
	lockStatusMutex sync.RWMutex
	lockStatus      bool
	lockRemoved     chan bool
	curIndex        uint64 // modificationIndex of etcd value
	curIndexMutex   sync.RWMutex
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

func NewLockManager(kapi *client.KeysAPI, path string) *LockManager {
	ans := new(LockManager)
	ans.kapi = kapi
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

func (lm *LockManager) lockGetter() {
	// always work towards getting the lock
	for {
		// read lock
		lock, err := lm.readLock()
		if err != nil {
			//TODO: if the error is that the key doesn't exist
			// if it doesn't exist -- attempt to set it
			fmt.Println("lock missing...")
			lm.createLock()
			// on success or failure, start the loop again
			continue
		}

		// if we have it, keep it updated
		if lock == lm.id {
			go lm.lockKeeper()
		}

		fmt.Printf("goroutines: %+v\n", runtime.NumGoroutine())

		// if another instance has it -- block until expired/deleted
		fmt.Println("waiting for lock removal...")
		<-lm.lockRemoved
	}
}

func (lm *LockManager) lockKeeper() {
	var err error = nil

	for err == nil && lm.HaveLock() {
		err = lm.updateTTL()
		lm.ttlSleep()
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
	resp, err := (*(lm.kapi)).Get(context.Background(), lm.path, &opts)

	// if trouble with etcd, return error
	// TODO: if lock missing, do not return err
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
	resp, err := (*(lm.kapi)).Set(context.Background(), lm.path, lm.id, &opts)
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
	resp, err := (*(lm.kapi)).Set(context.Background(), lm.path, lm.id, &opts)
	if err != nil {
		return err
	}

	lm.setCurIndex(resp.Index)
	lm.setLockStatus(true)
	return nil
}

func (lm *LockManager) HaveLock() bool {
	lm.lockStatusMutex.RLock()
	defer lm.lockStatusMutex.RUnlock()
	return lm.lockStatus
}

func (lm *LockManager) setLockStatus(val bool) {
	lm.lockStatusMutex.Lock()
	defer lm.lockStatusMutex.Unlock()
	lm.lockStatus = val
}

func (lm *LockManager) watchLock() {
	// start a watcher on the lock
	w := (*(lm.kapi)).Watcher(lm.path, nil)
	fmt.Println("watching:", lm.path)
	for {
		resp, err := w.Next(context.Background())
		if err != nil {
			// on etcd failure, w.Next will continue to try
			// and read the next watch message -- and when
			// etcd becomes available, this loop will continue
			// to operate correctly
			fmt.Printf("error: %+v\n", err)
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