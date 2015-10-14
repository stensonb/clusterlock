// distributed lock manager, using etcd
// provides a cluster-level "lock" which
// can be used when a cluster-level singleton
// is required.
// by default, loss of communication with etcd
// causes the lock manager to release the lock

package clusterlock

import (
	"fmt"
	"os"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"

	"github.com/stensonb/clusterlock/retryproxy"
)

type lockEvent int

const (
	ACQUIRED lockEvent = iota // used when we've acquired a lock
	REMOVED                   // used when the current lock is removed
	UNKNOWN                   // used when we lose comms with etcd cluster (and lock state is unknown)
)

type LockManager struct {
	ecrp               client.KeysAPI
	ecrpErrorChan      chan error
	path               string
	id                 string
	lock               *lock
	lockTTL            time.Duration
	lockStatus         bool
	lockStatusQuery    chan chan bool
	lockEvent          chan lockEvent
	needLock           chan bool
	closeAllGoRoutines chan interface{}
}

func (lm *LockManager) Shutdown() {
	// stop all goroutines
	close(lm.closeAllGoRoutines)
	close(lm.lockStatusQuery)
	close(lm.lockEvent)
	close(lm.needLock)

	if lm.lock != nil {
		lm.lock.Release()
	}
}

func NewLockManager(kapi client.KeysAPI, path string, ttl time.Duration) *LockManager {
	// initialization
	ans := new(LockManager)
	ans.ecrp = kapi

	// if it's a retryproxy.EtcdClientRetryProxy, set the error channel
	switch kapi := kapi.(type) {
	case *retryproxy.EtcdClientRetryProxy:
		ans.ecrpErrorChan = kapi.ErrorChan
	default:
		ans.ecrpErrorChan = nil
	}

	ans.path = path
	ans.lockTTL = ttl
	ans.lockStatusQuery = make(chan chan bool)
	ans.lockEvent = make(chan lockEvent)
	ans.needLock = make(chan bool)
	ans.closeAllGoRoutines = make(chan interface{})
	ans.id, _ = os.Hostname()
	ans.id += uuid.New()

	fmt.Println("Lock ID:", ans.id)

	go ans.lockEventHandler()        // respond when the lock changes
	go ans.ecrpErrorChannelHandler() // observe the retry proxy error channel
	go ans.lockWatcher()             // watch the lock in etcd
	go ans.lockGetter()              // get the lock
	ans.lockEvent <- UNKNOWN         // on start, the state of the lock is unknown

	return ans
}

func (lm *LockManager) lockEventHandler() {
	for {
		select {
		case evt := <-lm.lockEvent:
			// if removed, unknown, or acquired
			switch evt {
			case ACQUIRED:
				lm.lockStatus = true
				lm.lock = lm.newLock() // if acquired in etcd, create the data locally (and start ttlupdater, etc)
			case REMOVED, UNKNOWN:
				lm.lockStatus = false
				if lm.lock != nil {
					fmt.Println("lock isn't nil, so release the lock")
					lm.lock.Release()
					lm.lock = nil
				}
				lm.needLock <- true // notify we need the lock
			}
		case r := <-lm.lockStatusQuery:
			// return the lock status on the supplied channel
			r <- lm.lockStatus
		case <-lm.closeAllGoRoutines:
			return
		}
	}
}

func (lm *LockManager) HaveLock() bool {
	ans := make(chan bool)
	defer close(ans)
	lm.lockStatusQuery <- ans
	return <-ans
}

func (lm *LockManager) lockGetter() {
	alreadyLooking := false
	gotLock := make(chan interface{})

	for {
		select {
		case <-lm.needLock:
			if !alreadyLooking {
				// only need one instance looking for a lock
				alreadyLooking = true
				go lm.getLock(gotLock)
			}
		case <-gotLock:
			alreadyLooking = false
		case <-lm.closeAllGoRoutines:
			return
		}
	}
}

func (lm *LockManager) getLock(done chan interface{}) {
	// work towards getting the lock
	var thelock string
	var err error

	// loop until we've read the lock
	for thelock, err = lm.readLock(); err != nil; thelock, err = lm.readLock() {
		// if the key/lock doesn't exist -- attempt to set it
		fmt.Println("lock missing...")
		err2 := lm.createLock()
		if err2 != nil {
			fmt.Printf("failed to createLock(): %+v\n", err2)
		}
	}

	// if we have it, report it
	if thelock == lm.id {
		lm.lockEvent <- ACQUIRED
	}

	// either we have the lock, or another instance does
	// either way, we're done
	done <- true
}

type lock struct {
	lm           *LockManager
	eventChannel chan lockEvent
	lockKeeper   *lockKeeper
}

func (lm *LockManager) newLock() *lock {
	ans := new(lock)
	ans.lm = lm
	ans.eventChannel = make(chan lockEvent)
	ans.lockKeeper = ans.NewLockKeeper(lm.lockTTL / 2)
	return ans
}

// all done with the lock, release it
func (l *lock) Release() {
	l.lockKeeper.Stop() // lockKeeper cleanup function
	close(l.eventChannel)
	//TODO:	go l.deleteLock()
}

func (lm *LockManager) readLock() (string, error) {
	// read lock entry in etcd
	opts := client.GetOptions{Quorum: true}
	resp, err := lm.ecrp.Get(context.Background(), lm.path, &opts)

	// if lock missing, return err
	if err != nil {
		return "", err
	}

	return resp.Node.Value, nil
}

func (lm *LockManager) createLock() error {
	// write lm.id to lm.path with lm.lockTTL in etcd,
	// ensuring there is no previous value
	opts := client.SetOptions{TTL: lm.lockTTL, PrevExist: client.PrevNoExist}
	_, err := lm.ecrp.Set(context.Background(), lm.path, lm.id, &opts)
	return err
}

// watch ecrp error channel, and report UNKNOWN if it disappears
func (lm *LockManager) ecrpErrorChannelHandler() {
	for {
		select {
		case <-lm.ecrpErrorChan:
			fmt.Println("etcd client retry proxy had error...")
			lm.lockEvent <- UNKNOWN
		case <-lm.closeAllGoRoutines:
			return
		}
	}
}

func (lm *LockManager) lockWatcher() {
	// start a watcher on the lock
	w := lm.ecrp.Watcher(lm.path, nil)

	fmt.Println("watching:", lm.path)
	for {
		resp, err := w.Next(context.Background())
		if err != nil {
			// retry proxy ensures etcd is available
			// before returning...so, this error must
			// be something other than "cluster unavailable"
			fmt.Printf("error: %+v\n", err)

			// continue the loop
			continue
		}

		switch resp.Action {
		case "expire", "delete":
			fmt.Println("expired or deleted")
			lm.lockEvent <- REMOVED
		}
	}
}

type lockKeeper struct {
	l           *lock
	quit        chan interface{}
	sleepTicker time.Ticker
	curIndex    uint64 // modificationIndex of etcd value
}

func (l *lock) NewLockKeeper(renewPeriod time.Duration) *lockKeeper {
	ans := new(lockKeeper)
	ans.l = l
	ans.quit = make(chan interface{})

	sleepTicker := time.NewTicker(renewPeriod)

	go func() {
		for {
			select {
			case <-sleepTicker.C:
				ans.updateTTL()
			case <-ans.quit:
				fmt.Println("quitting the updatettl loop...")
				return
			}
		}
	}()

	return ans

}

func (lk *lockKeeper) Stop() {
	fmt.Println("telling the lockKeeper to stop()")
	lk.sleepTicker.Stop()
	close(lk.quit) // closing the quit channel ensures it gets selected
}

func (lk *lockKeeper) updateTTL() error {
	fmt.Println("updating TTL...")

	// set the ttl, ensuring the lock already exists
	// and that verify PrevIndex is what we expect
	opts := client.SetOptions{TTL: lk.l.lm.lockTTL, PrevValue: lk.l.lm.id, PrevExist: client.PrevExist, PrevIndex: lk.curIndex}
	resp, err := lk.l.lm.ecrp.Set(context.Background(), lk.l.lm.path, lk.l.lm.id, &opts)
	if err != nil {
		return err
	}
	lk.curIndex = resp.Index
	return nil
}
