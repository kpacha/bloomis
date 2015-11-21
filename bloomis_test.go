package bloomis

import (
	"fmt"

	"gopkg.in/redis.v3"
)

func ExampleAddAndTestValues() {
	client := getRedisClientForTesting()
	clean(client)
	bf, _ := NewSingleFilter(335477044, 23, client)
	fmt.Println(bf.AddToDefault([]byte("foo")))
	fmt.Println(bf.TestToDefault([]byte("foo")))
	fmt.Println(bf.TestToDefault([]byte("baz")))
	fmt.Println(bf.Add("unknown", []byte("foo")))
	fmt.Println(bf.Test("unknown", []byte("foo")))
	fmt.Println(bf.AddMulti(defaultKey, [][]byte{[]byte("multi1"), []byte("multi2")}))
	fmt.Println(bf.Test(defaultKey, []byte("multi1")))
	fmt.Println(bf.Test(defaultKey, []byte("multi2")))
	clean(client)
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

func ExampleLoadAndStoreMetadata() {
	client := getRedisClientForTesting()
	clean(client)

	bf1, _ := New(client)
	f1 := NewFilter("test1", 335477044, 23)
	f2 := NewFilter("test2", 335477044, 23)
	bf1.Filter = map[string]Filter{f1.Name: f1, f2.Name: f2}
	bf1.Save()
	fmt.Println(bf1.Add("test1", []byte("foo")))
	fmt.Println(bf1.Add("test2", []byte("bar")))

	bf2, _ := New(client)
	fmt.Println(len(bf2.Filter))
	fmt.Println(bf2.Test("test1", []byte("foo")))
	fmt.Println(bf2.Test("test2", []byte("bar")))
	fmt.Println(bf2.Test("unknown", []byte("foo")))
	clean(client)
	// Output:
	// <nil>
	// <nil>
	// 2
	// true <nil>
	// true <nil>
	// false The filter [unknown] doesn't exist!
}

func getRedisClientForTesting() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
}

func clean(client *redis.Client) {
	client.Del(metadataKey, prepareFilterKey(defaultKey))
}
