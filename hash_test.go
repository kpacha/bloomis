package bloomis

import (
	"fmt"
)

func ExampleHashing() {
	h := NewBloomisHasher()
	fmt.Println(h.GetHashes([]byte("hello")))
	// Output:
	// 8883723591023973575 11177612005948864433
}
