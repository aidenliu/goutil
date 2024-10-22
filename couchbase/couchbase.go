package couchbase

import (
	"errors"
	"fmt"
	"gopkg.in/couchbase/gocb.v1"
	"sync"
)

type Cb struct {
	*gocb.Bucket
}

type multiValueType struct {
	Key   string
	Value *string
}

type multiValueMapType struct {
	Key   string
	Value *map[string]interface{}
}

var lock sync.Mutex
var cbPool = map[string]Cb{}

func New(hostConfig, bucketConfig string) (*Cb, error) {
	poolKey := fmt.Sprintf("%s%s", hostConfig, bucketConfig)
	lock.Lock()
	defer lock.Unlock()

	cb, ok := cbPool[poolKey]
	if !ok || cb.Bucket == nil {
		cluster, err := gocb.Connect(hostConfig)
		if err != nil {
			return nil, err
		}

		bucket, err := cluster.OpenBucket(bucketConfig, "")
		if err != nil {
			return nil, err
		}

		cb = Cb{bucket}
		cbPool[poolKey] = cb
	}

	return &cb, nil
}

func (c *Cb) Get(key string, rv interface{}) error {
	_, err := c.Bucket.Get(key, rv)
	return err
}

func (c *Cb) GetMulti(keys []string) (error, []*multiValueType) {
	var r []*multiValueType
	var items []gocb.BulkOp
	var err error

	for _, k := range keys {
		var value string
		items = append(items, &gocb.GetOp{Key: k, Value: &value})
	}

	if err = c.Do(items); err == nil {
		for _, gv := range items {
			v := gv.(*gocb.GetOp)
			r = append(r, &multiValueType{Key: v.Key, Value: v.Value.(*string)})
		}
	}
	return err, r
}

func (c *Cb) GetMultiMap(keys []string) (error, []*multiValueMapType) {
	var r []*multiValueMapType
	var items []gocb.BulkOp
	var err error

	for _, k := range keys {
		var value map[string]interface{}
		items = append(items, &gocb.GetOp{Key: k, Value: &value})
	}

	loopCount := len(items) / 2000
	for i := 0; i <= loopCount; i++ {
		si := i * 2000
		se := si + 2000
		if se >= len(items) {
			se = len(items)
		}
		subItems := items[si:se]
		if err = c.Do(subItems); err == nil {
			var retryItems []gocb.BulkOp
			for _, gv := range subItems {
				v := gv.(*gocb.GetOp)
				if errors.Is(v.Err, gocb.ErrOverload) {
					retryItems = append(retryItems, v)
				} else {
					r = append(r, &multiValueMapType{Key: v.Key, Value: v.Value.(*map[string]interface{})})
				}
			}

			if len(retryItems) > 0 {
				if err = c.Do(retryItems); err == nil {
					for _, gv := range retryItems {
						v := gv.(*gocb.GetOp)
						r = append(r, &multiValueMapType{Key: v.Key, Value: v.Value.(*map[string]interface{})})
					}
				}
			}
		}
	}

	return err, r
}
