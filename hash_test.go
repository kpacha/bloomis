package bloomis

import (
	"fmt"
)

func ExampleHashing() {
	h := NewHasher()
	fmt.Println(h.getHashes([]byte("hello")))
	// Output:
	// 8883723591023973575 11177612005948864433
}
