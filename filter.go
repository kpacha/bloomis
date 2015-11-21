package bloomis

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"hash/fnv"

	"gopkg.in/redis.v3"
)

// Filter handles the bloom filter logic
type Filter struct {
	// h1 is the first hash function used to get the list of g1..gk values
	h1 hash.Hash
	// h2 is the second hash function used to get the list of g1..gk values
	h2 hash.Hash
	// m is the total number of bits of the bloom filter
	m uint64
	// k is the number of hash values
	k uint64
}

// NewFilter creates a new Bloom filter with _m_ bits and _k_ hashing values
// for the h1 function, it uses hash/fnv.New64() and, for h2,
// hash/crc64.New(crc64.MakeTable(crc64.ECMA))
func NewFilter(m, k uint64) Filter {
	return Filter{fnv.New64(), crc64.New(crc64.MakeTable(crc64.ECMA)), m, k}
}

// Add adds a new _value_ value to the Bloom filter called _key_ over the _client_ redis connection
func (bf *Filter) Add(key string, value []byte, client *redis.Client) error {
	return bf.AddMulti(key, [][]byte{value}, client)
}

// Add adds a set of _values_ values to the Bloom filter called _key_ over the _client_ redis connection
func (bf *Filter) AddMulti(key string, values [][]byte, client *redis.Client) error {
	bitset := bf.bitset(values)

	multi := client.Multi()
	_, err := multi.Exec(func() error {
		for bit := range bitset {
			multi.SetBit(key, bit, 1)
		}
		return nil
	})

	return err
}

// Test tests if a _value_ value has been inserted into the Bloom filter called _key_ over the _client_ redis connection
func (bf *Filter) Test(key string, value []byte, client *redis.Client) (bool, error) {
	return bf.TestMulti(key, [][]byte{value}, client)
}

// Test tests if all _values_ values have been inserted into the Bloom filter called _key_ over the _client_ redis connection
func (bf *Filter) TestMulti(key string, values [][]byte, client *redis.Client) (bool, error) {
	bitset := bf.bitset(values)

	for bit := range bitset {
		cmd := client.GetBit(key, bit)
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
	s := make([]uint64, 2)
	for i, h := range []hash.Hash{bf.h1, bf.h2} {
		h.Reset()
		h.Write(value)
		s[i] = binary.BigEndian.Uint64(h.Sum(nil))
	}

	b := make([]uint64, bf.k)
	for i := uint64(0); i < bf.k; i++ {
		b[i] = (s[0] + s[1]*uint64(i)) % bf.m
	}
	return b
}
