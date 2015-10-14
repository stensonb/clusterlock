# clusterlock

a cluster-level lock manager, backed by [etcd](http://etcd.io)

## how to use

```golang
import "github.com/stensonb/clusterlock"
```

then, create the instance:

```golang
cfg := client.Config{
  Endpoints: []string{"http://127.0.0.1:2379"},
  Transport: client.DefaultTransport,
  // set timeout per request to fail fast when the target endpoint is unavailable
  HeaderTimeoutPerRequest: time.Second,
}
c, _ := client.New(cfg)

ecrpErrorChan := make(chan error)
ecrp := retryproxy.NewEtcdClientRetryProxy(c, ecrpErrorChan, 1, 60)
path := "some-path-in-etcd" // the path in etcd where you want the lock to be stored

// the lockttl, in seconds
// a value too low increases network/etcd chatter
// a value too high results in longer periods of a "lockless" cluster
ttl := LockTTL

lm := clusterlock.NewLockManager(ecrp, ecrpErrorChan, path, ttl)
defer lm.Shutdown()  // let us clean-up the internals (ttlupdater, etc)

// now we can poll the LockManager to see if we have the lock:
if lm.HaveLock() {
  // do something cluster-level, as we're now guaranteed
  // to be the only instance of this library with the lock
  // path specified in etcd
}
```

## features
* HA (run multiple, and only one will return HaveLock()==true)
* retry logic built-in to all etcd calls
* when etcd is unavailable, HaveLock()==false
* lock is maintained by a single host ; and TTL periodically updated
* self-healing
  * if a single instance dies, the lock’s TTL will expire and other instances will fight for lock
  * if etcd fails, lock is immediately rescinded from all instances
  * if the lock in etcd is removed, all instances will fight for lock

# help me
if see a problem with this, please open an issue, or submit a PR...
items I'd like to improve include:
* bug squashing
* better/more testing
* better/more abstraction from etcd (to support other cluster-type data stores)
