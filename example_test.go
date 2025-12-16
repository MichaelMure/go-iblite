package iblt_test

import (
	"fmt"

	iblt "github.com/MichaelMure/go-iblite"
)

func Example() {
	// Let's perform a set reconciliation between Alice and Bob.
	// Each of them has a set of keys, and we want to find the keys that the other party doesn't have, so they can
	// synchronize the differences. This is a common problem in distributed systems, databases, etc.
	// In particular, it's useful in scenarios where the difference between two sets is small, but the total size of
	// the sets is large.

	// Each creates an IBLT large enough to hold the expected **difference** between the two sets.
	alice := iblt.NewKTable(20, 4)
	bob := iblt.NewKTable(20, 4)

	// Each inserts the keys they have into their respective IBLT.
	// Here, we insert 10 million keys, far, far more than the IBLT can hold without saturating.
	// Bob will have some keys missing and some extra keys compared to Alice.
	for i := uint64(0); i < 10_000_000; i++ {
		alice.Insert(i)
	}
	for i := uint64(5); i < 10_000_005; i++ {
		bob.Insert(i)
	}

	// Bob transmits his IBLT to Alice, and Alice subtracts it from her own.
	bobBytes := bob.ToBytes()
	received, err := iblt.KTableFromBytes(bobBytes)
	if err != nil {
		panic(err)
	}

	// Just to illustrate, we'll print the size of a million keys, and the size of the serialized IBLT.
	fmt.Printf("10 million keys: %d bytes\n", 10_000_000*8)
	fmt.Printf("IBLT size: %d bytes\n", len(bobBytes))

	// Now the magic trick: Alice subtracts the received IBLT from her own, and peel (decode) the missing keys.
	alice.Subtract(received)

	fmt.Println("Keys that Alice doesn't have:")
	for key := range alice.Copy().PeelMisses() {
		fmt.Println(key)
	}
	fmt.Println()
	fmt.Println("Keys that Bob doesn't have:")
	for key := range alice.Copy().PeelHas() {
		fmt.Println(key)
	}

	// Output:
	// 10 million keys: 80000000 bytes
	// IBLT size: 484 bytes
	// Keys that Alice doesn't have:
	// 10000003
	// 10000004
	// 10000002
	// 10000001
	//
	// Keys that Bob doesn't have:
	// 0
	// 1
	// 2
	// 4
}
