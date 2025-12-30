<div align="center">
  <h1 align="center">go-iblite</h1>

  <p>
    <a href="https://github.com/MichaelMure/go-iblite/tags">
        <img alt="GitHub Tag" src="https://img.shields.io/github/v/tag/MichaelMure/go-iblite">
    </a>
    <a href="https://github.com/MichaelMure/go-iblite/actions?query=">
      <img src="https://github.com/MichaelMure/go-iblite/actions/workflows/gotest.yml/badge.svg" alt="Build Status">
    </a>
    <a href="https://MichaelMure.github.io/go-iblite/dev/bench/">
        <img alt="Go benchmarks" src="https://img.shields.io/badge/Benchmarks-go-blue">
    </a>
    <a href="https://github.com/MichaelMure/go-iblite/blob/v1/LICENSE">
        <img alt="MIT License" src="https://img.shields.io/badge/License-MIT-green">
    </a>
    <a href="https://pkg.go.dev/github.com/MichaelMure/go-iblite">
      <img src="https://img.shields.io/badge/Docs-godoc-blue" alt="Docs">
    </a>
  </p>
</div>

## Overview

This is an implementation of an Invertible Bloom Lookup Table in Go. The name comes from IBLT lite; in both sense of simple and efficient. This package implements two variants of IBLT: key-only and key-value.

[Invertible Bloom Lookup Tables](https://arxiv.org/abs/1101.2245), introduced by Michael T. Goodrich, Michael Mitzenmacher are a probabilistic data structure that compactly represents a set and can be decoded to recover its elements, provided the structure is not too full.

Unlike a standard Bloom filter, an IBLT is invertible: it stores enough aggregate information to reconstruct individual entries via a peeling process.

IBLTs are commonly used for set reconciliation between two parties. By building an IBLT from each set and subtracting them, the result encodes only the set difference, which can be decoded efficiently. This allows large sets with small differences to be synchronized with communication proportional to the difference size, not the total set size.

For a more detailed explanation of the algorithm, see [b5's great explanation video](https://www.youtube.com/watch?v=BIN2a-CIvNA).

## Drawbacks

IBLTs are great but still require to dimension them beforehand, proportionally to the **set difference**, which can be a challenge in some cases. A great extension of that idea are [Rateless IBLTs](https://arxiv.org/abs/2402.02668), which are not implemented here.

## Example

```go
// Let's perform a set reconciliation between Alice and Bob.
// Each of them has a set of keys, and we want to find the keys that the other party doesn't
// have, so they can synchronize the differences. This is a common problem in distributed
// systems, databases, etc. In particular, it's useful in scenarios where the difference between
// two sets is small, but the total size of the sets is large.

// Each creates an IBLT large enough to hold the expected **difference** between the two sets.
alice := iblt.NewKTable(20, 4)
bob := iblt.NewKTable(20, 4)

// Each inserts the keys they have into their respective IBLT.
// Here, we insert 1 million keys, far, far more than the IBLT can hold without saturating.
// Bob will have some keys missing and some extra keys compared to Alice.
for i := uint64(0); i < 1_000_000; i++ {
    alice.Insert(i)
}
for i := uint64(5); i < 1_000_005; i++ {
    bob.Insert(i)
}

// Bob transmits his IBLT to Alice, and Alice subtracts it from her own.
bobBytes := bob.ToBytes()
received, err := iblt.KTableFromBytes(bobBytes)
if err != nil {
    panic(err)
}

// Just to illustrate, we'll print the size of a million keys, and the size of the IBLT.
fmt.Printf("1 million keys: %d bytes\n", 1_000_000*8)
fmt.Printf("IBLT size: %d bytes\n", len(bobBytes))

// Now the magic trick:
// Alice subtracts the received IBLT from her own, and peel (decode) the missing keys.
alice.Subtract(received)

fmt.Println()
fmt.Println("Keys that Alice doesn't have:")
misses := alice.Copy()
for key := range misses.PeelMisses() {
fmt.Println(key)
}
fmt.Println("Peeling completed:", misses.Empty())
fmt.Println()
fmt.Println("Keys that Bob doesn't have:")
has := bob.Copy()
for key := range alice.Copy().PeelHas() {
fmt.Println(key)
}
fmt.Println("Peeling completed:", has.Empty())

// Output:
// 1 million keys: 8000000 bytes
// IBLT size: 484 bytes
//
// Keys that Alice doesn't have:
// 1000000
// 1000004
// 1000003
// 1000002
// 1000001
// Peeling completed: true
//
// Keys that Bob doesn't have:
// 3
// 0
// 4
// 1
// 2
// Peeling completed: false
```

## License

MIT