package bloomis

import (
	"fmt"
	"hash/crc64"
	"hash/fnv"

	"gopkg.in/redis.v3"
)

func ExampleHashing() {
	f := Filter{fnv.New64(), crc64.New(crc64.MakeTable(crc64.ECMA)), 335477044, 23}
	fmt.Println(f.bits([]byte("hello")))
	// Output:
	// [93375935 300021868 195851017 67019906 298326099 169494988 40663877 271970070 143138959 38968108 245614041 116782930 12612079 219258012 115087161 321733094 192901983 88731132 295377065 191206214 62375103 269021036 164850185]
}

func ExampleAddAndTestValues() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	bf := NewSingleFilter(335477044, 23, client)
	fmt.Println(bf.AddToDefault([]byte("foo")))
	fmt.Println(bf.TestToDefault([]byte("foo")))
	fmt.Println(bf.TestToDefault([]byte("baz")))
	fmt.Println(bf.Add("unknown", []byte("foo")))
	fmt.Println(bf.Test("unknown", []byte("foo")))
	fmt.Println(bf.AddMulti(defaultKey, [][]byte{[]byte("multi1"), []byte("multi2")}))
	fmt.Println(bf.Test(defaultKey, []byte("multi1")))
	fmt.Println(bf.Test(defaultKey, []byte("multi2")))
	// Output:
	// <nil>
	// true <nil>
	// false <nil>
	// The filter [unknown] doesn't exist!
	// false The filter [unknown] doesn't exist!
	// <nil>
	// true <nil>
	// true <nil>
}
