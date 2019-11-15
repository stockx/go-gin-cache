package persistence

import (
	"net"
	"testing"
	"time"

	"github.com/go-redis/redis"
)

// These tests require redis server running on localhost:6379 (the default)
const redisTestServer = "localhost:6379"

var newRedisStore = func(t *testing.T, defaultExpiration time.Duration) CacheStore {
	c, err := net.Dial("tcp", redisTestServer)
	if err == nil {
		c.Write([]byte("flush_all\r\n"))
		c.Close()
		redisCache := NewRedisCache(&RedisConfig{
			ReaderOptions: &redis.Options{
				Addr:         ":6379",
				DialTimeout:  10 * time.Second,
				ReadTimeout:  30 * time.Second,
				WriteTimeout: 30 * time.Second,
				PoolSize:     10,
				PoolTimeout:  30 * time.Second,
				Password:     "",
				DB:           0,
			},
			WriterOptions: &redis.Options{
				Addr:         ":6379",
				DialTimeout:  10 * time.Second,
				ReadTimeout:  30 * time.Second,
				WriteTimeout: 30 * time.Second,
				PoolSize:     10,
				PoolTimeout:  30 * time.Second,
				Password:     "",
				DB:           0,
			},
			DefaultExpiration: 0 * time.Second,
		})
		redisCache.Flush()
		return redisCache
	}
	t.Errorf("couldn't connect to redis on %s", redisTestServer)
	t.FailNow()
	panic("")
}

func TestRedisCache_TypicalGetSet(t *testing.T) {
	typicalGetSet(t, newRedisStore)
}

func TestRedisCache_IncrDecr(t *testing.T) {
	incrDecr(t, newRedisStore)
}

func TestRedisCache_Expiration(t *testing.T) {
	expiration(t, newRedisStore)
}

func TestRedisCache_EmptyCache(t *testing.T) {
	emptyCache(t, newRedisStore)
}

func TestRedisCache_Replace(t *testing.T) {
	testReplace(t, newRedisStore)
}

func TestRedisCache_Add(t *testing.T) {
	testAdd(t, newRedisStore)
}
