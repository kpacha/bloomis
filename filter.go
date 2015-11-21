package bloomis

import (
	"fmt"

	"gopkg.in/redis.v3"
)

// Filter handles the bloom filter logic
type Filter struct {
	// Name of the filter
	Name string `json:"name"`
	// hasher is a pointer to the struct containing the pair of hashing functions used to get the list of g1..gk values
	hasher *hasher `json:"-"`
	// M is the total number of bits of the bloom filter
	M uint64 `json:"m"`
	// K is the number of hash values
	K uint64 `json:"k"`
}

var defaultFilterPrefix = "bloomis_f_"

func prepareFilterKey(key string) string {
	return fmt.Sprintf("%s_%s", defaultFilterPrefix, key)
}

// NewFilter creates a new Bloom filter with the given _name_ name, _m_ bits and _k_ hashing values
// for the h1 function, it uses hash/fnv.New64() and, for h2, hash/crc64.New(crc64.MakeTable(crc64.ECMA))
func NewFilter(name string, m, k uint64) Filter {
	return Filter{name, NewHasher(), m, k}
}

// Add adds a new _value_ value to the Bloom filter called _key_ over the _client_ redis connection
func (bf *Filter) Add(key string, value []byte, client *redis.Client) error {
	return bf.AddMulti(key, [][]byte{value}, client)
}

// AddMulti adds a set of _values_ values to the Bloom filter called _key_ over the _client_ redis connection
func (bf *Filter) AddMulti(key string, values [][]byte, client *redis.Client) error {
	bitset := bf.bitset(values)

	bloomisKey := prepareFilterKey(key)
	multi := client.Multi()
	_, err := multi.Exec(func() error {
		for bit := range bitset {
			multi.SetBit(bloomisKey, bit, 1)
		}
		return nil
	})

	return err
}

// Test tests if a _value_ value has been inserted into the Bloom filter called _key_ over the _client_ redis connection
func (bf *Filter) Test(key string, value []byte, client *redis.Client) (bool, error) {
	return bf.TestMulti(key, [][]byte{value}, client)
}

// TestMulti tests if all _values_ values have been inserted into the Bloom filter called _key_ over the _client_ redis connection
func (bf *Filter) TestMulti(key string, values [][]byte, client *redis.Client) (bool, error) {
	bitset := bf.bitset(values)

	bloomisKey := prepareFilterKey(key)
	for bit := range bitset {
		cmd := client.GetBit(bloomisKey, bit)
		if cmd == nil {
			return false, fmt.Errorf("Error getting the bit %d", bit)
		}
		if cmd.Val() == 0 {
			return false, nil
		}
	}

	return true, nil
}

func (bf *Filter) bitset(values [][]byte) map[int64]bool {
	bitset := map[int64]bool{}
	for _, v := range values {
		for _, b := range bf.bits(v) {
			bitset[int64(b)] = true
		}
	}
	return bitset
}

func (bf *Filter) bits(value []byte) []uint64 {
	s0, s1 := bf.hasher.getHashes(value)

	b := make([]uint64, bf.K)
	for i := uint64(0); i < bf.K; i++ {
		b[i] = (s0 + s1*uint64(i)) % bf.M
	}
	return b
}
