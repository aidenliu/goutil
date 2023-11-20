package memcached

import (
	"github.com/aidenliu/goutil/config"
	"github.com/bradfitz/gomemcache/memcache"
	"strings"
)

type Client struct {
	c *memcache.Client
}

func New(configKey string) *Client {
	mConfig := config.Service(configKey)
	hosts := strings.Split(mConfig["host"], ",")
	client := memcache.New(hosts...)
	return &Client{c: client}
}

// Get 获取值
func (m *Client) Get(key string) []byte {
	r, err := m.c.Get(key)
	if err == nil {
		return r.Value
	} else {
		return nil
	}
}

// Set 设置值
func (m *Client) Set(key string, value []byte, expire int32) error {
	valueItem := memcache.Item{Key: key, Value: value, Expiration: expire}
	return m.c.Set(&valueItem)
}

// Del 删除值
func (m *Client) Del(prefixCode, key string) error {
	return m.c.Delete(key)
}

// Incre 递增值
func (m *Client) Incre(prefixCode, key string, num uint64) (uint64, error) {
	return m.c.Increment(key, num)
}

// Decre 递减值
func (m *Client) Decre(prefixCode, key string, num uint64) (uint64, error) {
	return m.c.Decrement(key, num)
}
