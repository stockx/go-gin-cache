package persistence

import (
	"time"

	"github.com/gin-contrib/cache/utils"
	"github.com/go-redis/redis"
)

type RedisConfig struct {
	WriterOptions     *redis.Options
	ReaderOptions     *redis.Options
	Password          string
	DefaultExpiration time.Duration
}

// RedisStore represents the cache with redis persistence
type RedisStore struct {
	writer            *redis.Client
	reader            *redis.Client
	defaultExpiration time.Duration
}

var DefaultRedisOption = &redis.Options{
	Addr:         ":6379",
	DialTimeout:  10 * time.Second,
	ReadTimeout:  30 * time.Second,
	WriteTimeout: 30 * time.Second,
	PoolSize:     10,
	PoolTimeout:  30 * time.Second,
	Password:     "",
	DB:           0,
}

// NewRedisCache returns a RedisStore
func NewRedisCache(config *RedisConfig) *RedisStore {
	writer := redis.NewClient(config.WriterOptions)
	reader := redis.NewClient(config.ReaderOptions)
	return &RedisStore{writer, reader, config.DefaultExpiration}
}

// Set (see CacheStore interface)
func (c *RedisStore) Set(key string, value interface{}, expires time.Duration) error {
	return c.invoke(c.writer.Do, key, value, expires)
}

// Add (see CacheStore interface)
func (c *RedisStore) Add(key string, value interface{}, expires time.Duration) error {
	if exists(c.reader, key) {
		return ErrNotStored
	}
	return c.invoke(c.writer.Do, key, value, expires)
}

// Replace (see CacheStore interface)
func (c *RedisStore) Replace(key string, value interface{}, expires time.Duration) error {
	if !exists(c.reader, key) {
		return ErrNotStored
	}
	err := c.invoke(c.writer.Do, key, value, expires)
	if value == nil {
		return ErrNotStored
	}

	return err
}

// Get (see CacheStore interface)
func (c *RedisStore) Get(key string, ptrValue interface{}) error {
	cmd := redis.NewStringCmd("GET", key)
	err := c.reader.Process(cmd)
	if cmd.Err() != nil {
		return ErrCacheMiss
	}
	item, err := cmd.Bytes()
	if err != nil {
		return err
	}
	return utils.Deserialize(item, ptrValue)
}

func exists(client *redis.Client, key string) bool {
	retval, _ := client.Do("EXISTS", key).Bool()
	return retval
}

// Delete (see CacheStore interface)
func (c *RedisStore) Delete(key string) error {
	if !exists(c.reader, key) {
		return ErrCacheMiss
	}
	return c.writer.Do("DEL", key).Err()
}

// Increment (see CacheStore interface)
func (c *RedisStore) Increment(key string, delta uint64) (uint64, error) {
	// Check for existance *before* increment as per the cache contract.
	// redis will auto create the key, and we don't want that. Since we need to do increment
	// ourselves instead of natively via INCRBY (redis doesn't support wrapping), we get the value
	// and do the exists check this way to minimize calls to Redis
	cmd := c.reader.Do("GET", key)
	if cmd.Val() == nil {
		return 0, ErrCacheMiss
	}
	if cmd.Err() == nil {
		currentVal, err := cmd.Int64()
		if err != nil {
			return 0, err
		}
		sum := currentVal + int64(delta)
		cmd = c.writer.Do("SET", key, sum)
		if cmd.Err() != nil {
			return 0, err
		}
		return uint64(sum), nil
	}

	return 0, cmd.Err()
}

// Decrement (see CacheStore interface)
func (c *RedisStore) Decrement(key string, delta uint64) (newValue uint64, err error) {
	// Check for existance *before* increment as per the cache contract.
	// redis will auto create the key, and we don't want that, hence the exists call
	if !exists(c.writer, key) {
		return 0, ErrCacheMiss
	}
	// Decrement contract says you can only go to 0
	// so we go fetch the value and if the delta is greater than the amount,
	// 0 out the value
	currentVal, err := c.reader.Do("GET", key).Int64()
	if err == nil && delta > uint64(currentVal) {
		tempint, err := c.writer.Do("DECRBY", key, currentVal).Int64()
		return uint64(tempint), err
	}
	tempint, err := c.writer.Do("DECRBY", key, delta).Int64()
	return uint64(tempint), err
}

// Flush (see CacheStore interface)
func (c *RedisStore) Flush() error {
	cmd := c.writer.Do("FLUSHALL")
	return cmd.Err()
}

func (c *RedisStore) invoke(f func(args ...interface{}) *redis.Cmd,
	key string, value interface{}, expires time.Duration) error {

	switch expires {
	case DEFAULT:
		expires = c.defaultExpiration
	case FOREVER:
		expires = time.Duration(0)
	}

	b, err := utils.Serialize(value)
	if err != nil {
		return err
	}

	if expires > 0 {
		cmd := f("SETEX", key, int32(expires/time.Second), b)
		return cmd.Err()
	}

	cmd := f("SET", key, b)
	return cmd.Err()

}
