package bloomis

import (
	"encoding/binary"
	"hash"
	"hash/crc64"
	"hash/fnv"
)

type hasher struct {
	// h1 is the first hash function used to get the list of g1..gk values
	h1 hash.Hash
	// h2 is the second hash function used to get the list of g1..gk values
	h2 hash.Hash
}

func NewHasher() *hasher {
	return &hasher{fnv.New64(), crc64.New(crc64.MakeTable(crc64.ECMA))}
}

func (this *hasher) getHashes(value []byte, m, k uint64) (uint64, uint64) {
	s := make([]uint64, 2)
	for i, h := range []hash.Hash{this.h1, this.h2} {
		h.Reset()
		h.Write(value)
		s[i] = binary.BigEndian.Uint64(h.Sum(nil))
	}

	return s[0], s[1]
}
