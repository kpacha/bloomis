package bloomis

import (
	"fmt"

	"gopkg.in/redis.v3"
)

// BloomFilter is a collection of bloom filters and a connection to a redis service
type BloomFilter struct {
	// Filter is the collection of named bloom filters
	Filter map[string]Filter
	// Client is a pointer to the redis client
	Client *redis.Client
}

// defaultKey is the key to use with BloomFilter with a single filter
var defaultKey = "defaultBloomFilter"

// New creates a new Bloom filter against the _client_ redis client
func New(client *redis.Client) *BloomFilter {
	return &BloomFilter{map[string]Filter{}, client}
}

// NewSingleFilter creates a new Bloom filter against the _client_ redis client,
// with defaultKey as name, _m_ bits and _k_ hashing values
// for the h1 function, it uses hash/fnv.New64() and, for h2,
// hash/crc64.New(crc64.MakeTable(crc64.ECMA))
func NewSingleFilter(m, k uint64, client *redis.Client) *BloomFilter {
	bf := New(client)
	bf.Filter[defaultKey] = NewFilter(defaultKey, m, k)
	return bf
}

// Add adds a new _value_ value to the default Bloom filter
func (bf *BloomFilter) AddToDefault(value []byte) error {
	if len(bf.Filter) > 1 {
		return fmt.Errorf("The filter has more than one key")
	}
	return bf.Add(defaultKey, value)
}

// Add adds a new _value_ value to the Bloom filter called _key_
func (bf *BloomFilter) Add(key string, value []byte) error {
	return bf.AddMulti(key, [][]byte{value})
}

// Add adds new _values_ values to the Bloom filter called _key_
func (bf *BloomFilter) AddMulti(key string, values [][]byte) error {
	f, ok := bf.Filter[key]
	if !ok {
		return fmt.Errorf("The filter [%s] doesn't exist!", key)
	}
	return f.AddMulti(key, values, bf.Client)
}

// Test tests if a _value_ value has been inserted into the default Bloom filter
func (bf *BloomFilter) TestToDefault(value []byte) (bool, error) {
	if len(bf.Filter) > 1 {
		return false, fmt.Errorf("The filter has more than one key")
	}
	return bf.Test(defaultKey, value)
}

// Test tests if a _value_ value has been inserted into the Bloom filter called _key_
func (bf *BloomFilter) Test(key string, value []byte) (bool, error) {
	return bf.TestMulti(key, [][]byte{value})
}

// Test tests if all _values_ values have been inserted into the Bloom filter called _key_
func (bf *BloomFilter) TestMulti(key string, values [][]byte) (bool, error) {
	f, ok := bf.Filter[key]
	if !ok {
		return false, fmt.Errorf("The filter [%s] doesn't exist!", key)
	}
	return f.TestMulti(key, values, bf.Client)
}
