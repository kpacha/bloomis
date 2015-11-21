package bloomis

import (
	"encoding/json"
	"fmt"

	"gopkg.in/redis.v3"
)

// BloomFilter is a collection of bloom filters and a connection to a redis service
type BloomFilter struct {
	// Filter is the collection of named bloom filters
	Filter map[string]Filter `json:"filters"`
	// Client is a pointer to the redis client
	Client *redis.Client `json:"-"`
}

// defaultKey is the key to use with BloomFilter with a single filter
var defaultKey = "defaultBloomFilter"

// metadataKey is the key where the BloomFilter metadata is stored
var metadataKey = "bloomis_metadata"

// New creates a new Bloom filter against the _client_ redis client
func New(client *redis.Client) (*BloomFilter, error) {
	bf := &BloomFilter{map[string]Filter{}, client}
	total, err := bf.Init()
	if err != nil {
		return nil, err
	}
	if total == 0 {
		if err := bf.Save(); err != nil {
			return nil, err
		}
	}
	return bf, err
}

// NewSingleFilter creates a new Bloom filter against the _client_ redis client,
// with defaultKey as name, _m_ bits and _k_ hashing values
// for the h1 function, it uses hash/fnv.New64() and, for h2,
// hash/crc64.New(crc64.MakeTable(crc64.ECMA))
func NewSingleFilter(m, k uint64, client *redis.Client) (*BloomFilter, error) {
	bf, err := New(client)
	if err != nil {
		return bf, err
	}
	if len(bf.Filter) != 0 {
		return bf, fmt.Errorf("Error creating new BloomFilter with a single filter: There are filters already registered")
	}
	bf.Filter[defaultKey] = NewFilter(defaultKey, m, k)
	if err := bf.Save(); err != nil {
		return nil, err
	}
	return bf, nil
}

// Init loads the metadata stored in the redis server and creates the required filters
func (bf *BloomFilter) Init() (int, error) {
	meta, err := bf.Client.Get(metadataKey).Result()
	if err == redis.Nil {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	loaded := &BloomFilter{map[string]Filter{}, nil}
	err = json.Unmarshal([]byte(meta), &loaded)
	if err != nil {
		return 0, err
	}
	for name, f := range loaded.Filter {
		bf.Filter[name] = NewFilter(name, f.M, f.K)
	}
	return len(bf.Filter), nil
}

// Save saves the metadata in the redis server
func (bf *BloomFilter) Save() error {
	meta, err := json.Marshal(*bf)
	if err != nil {
		return err
	}
	return bf.Client.Set(metadataKey, meta, 0).Err()
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
