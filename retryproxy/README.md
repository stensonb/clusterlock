# retryproxy

an etcd retry client which retries against etcd when required

# how to use

```
import "github.com/stensonb/clusterlock/retryproxy"
```

then, create a retryproxy instead of the etcd keysAPI:

```
// two options here:
// 1. if you don't want to be notified when the retry proxy needs to retry, pass ec=nil
// 2. if you want to follow when the retry client had to retry, pass an error channel via ec
// NOTE: be sure to read from the error channel (ec), otherwise, you will block the retry
// min uint ; the shortest amount of time to wait (in seconds) on failure
// max uint ; the longest amount of time to wait (in seconds) on failure
c, _ := client.New(cfg)  // a new etcd client
var ec chan error = nil
min := uint(1)
max := uint(60)
ecrp := retryproxy.NewEtcdClientRetryProxy(c, ec, min, max)

// now, use the ecrp just as you would a client.KeysAPI from etcd
// read value from etcd
opts := client.GetOptions{Quorum: true}
resp, err := ecrp.Get(context.Background(), somepath, &opts)
```

# features

* client.ErrClusterUnavailable errors from ANY call results in an increase in wait time before trying again
* ANY successful etcd call results in a removal of all wait times for all other etcd calls
* error channel to monitor each retry
* support a range minimum and maximum wait times before trying again
* failed etcd calls roughly double the next call's wait time
