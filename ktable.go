package iblt

import (
	"encoding/binary"
	"fmt"
	"io"
	"iter"
)

// KTable is an Invertible Bloom Lookup Table, which only holds keys, not values.
// It's a probabilistic concise data structure for set representation that supports a peeling operation as the recovery of the elements in the represented set.
// It can be used in particular for set reconciliation, see the example.
// KTable is NOT thread safe.
type KTable struct {
	buckets   []kBucket
	hashCount int // number of hashes applied to each key
}

type kBucket struct {
	idSum   uint64 // bitwise XOR of all keys in the bucket
	hashSum uint64 // bitwise XOR of all hashes of keys in the bucket
	count   int64  // number of elements in the bucket
}

// NewKTable creates a new key-only IBLT with the given parameters.
// The hashCount parameter is the number of hashes applied to each key.
// The bucketCount parameter is the number of buckets.
// Those parameters directly affect the memory usage of the IBLT, as well as the probability to succeed in a peeling operation.
func NewKTable(bucketCount, hashCount int) *KTable {
	return &KTable{
		buckets:   make([]kBucket, bucketCount),
		hashCount: hashCount,
	}
}

func FromBytes(data []byte) (*KTable, error) {
	if len(data) < 2+2 {
		return nil, fmt.Errorf("not enough data for header: %d", len(data))
	}
	hashCount := binary.BigEndian.Uint16(data[0:2])
	bucketCount := binary.BigEndian.Uint16(data[2:4])
	data = data[4:]

	res := NewKTable(int(bucketCount), int(hashCount))

	if len(data) < int(bucketCount)*24 {
		return nil, fmt.Errorf("not enough data for buckets: %d", len(data))
	}

	for i := 0; i < int(bucketCount); i++ {
		res.buckets[i].idSum = binary.BigEndian.Uint64(data[0:8])
		res.buckets[i].hashSum = binary.BigEndian.Uint64(data[8:16])
		res.buckets[i].count = int64(binary.BigEndian.Uint64(data[16:24]))
		data = data[24:]
	}
	return res, nil
}

func FromReader(r io.Reader) (*KTable, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("not enough data for header: %w", err)
	}
	hashCount := int(binary.BigEndian.Uint16(header[0:2]))
	bucketCount := int(binary.BigEndian.Uint16(header[2:4]))

	res := NewKTable(bucketCount, hashCount)

	var buf [24]byte
	for i := 0; i < bucketCount; i++ {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return nil, fmt.Errorf("not enough data for buckets: %w", err)
		}
		res.buckets[i].idSum = binary.BigEndian.Uint64(buf[0:8])
		res.buckets[i].hashSum = binary.BigEndian.Uint64(buf[8:16])
		res.buckets[i].count = int64(binary.BigEndian.Uint64(buf[16:24]))
	}
	return res, nil
}

// ToBytes returns the serialized representation of the key-only IBLT.
func (t *KTable) ToBytes() []byte {
	data := make([]byte, 0, 2+2+len(t.buckets)*3*8)
	data = binary.BigEndian.AppendUint16(data, uint16(t.hashCount))
	data = binary.BigEndian.AppendUint16(data, uint16(len(t.buckets)))
	for _, bucket := range t.buckets {
		data = binary.BigEndian.AppendUint64(data, bucket.idSum)
		data = binary.BigEndian.AppendUint64(data, bucket.hashSum)
		data = binary.BigEndian.AppendUint64(data, uint64(bucket.count))
	}
	return data
}

// ToWriter serializes the IBLT to the given writer.
func (t *KTable) ToWriter(w io.Writer) error {
	var header [4]byte
	binary.BigEndian.PutUint16(header[0:2], uint16(t.hashCount))
	binary.BigEndian.PutUint16(header[2:4], uint16(len(t.buckets)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	var buf [24]byte
	for _, bucket := range t.buckets {
		binary.BigEndian.PutUint64(buf[0:8], bucket.idSum)
		binary.BigEndian.PutUint64(buf[8:16], bucket.hashSum)
		binary.BigEndian.PutUint64(buf[16:24], uint64(bucket.count))
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
	}
	return nil
}

// Copy returns a deep copy of the IBLT.
func (t *KTable) Copy() *KTable {
	res := &KTable{buckets: make([]kBucket, len(t.buckets)), hashCount: t.hashCount}
	for i := range t.buckets {
		res.buckets[i] = t.buckets[i]
	}
	return res
}

// Equals returns true if the two IBLTs are identical.
func (t *KTable) Equals(other *KTable) bool {
	if !t.CompatibleWith(other) {
		return false
	}
	for i := range t.buckets {
		if t.buckets[i] != other.buckets[i] {
			return false
		}
	}
	return true
}

// Insert adds the given key to the IBLT.
func (t *KTable) Insert(key uint64) {
	h, indices := t.indices(key)
	for idx := range indices {
		t.buckets[idx].idSum ^= key
		t.buckets[idx].hashSum ^= h
		t.buckets[idx].count++
	}
}

// Delete removes the given key from the IBLT.
func (t *KTable) Delete(key uint64) {
	h, indices := t.indices(key)
	for idx := range indices {
		t.buckets[idx].idSum ^= key
		t.buckets[idx].hashSum ^= h
		t.buckets[idx].count--
	}
}

// Count returns the number of keys in the IBLT.
// IMPORTANT: this number is only accurate if:
// - no Subtract has been done
// - no Delete with a key not in the IBLT has been done
func (t *KTable) Count() int {
	var sum int64
	for _, bucket := range t.buckets {
		sum += bucket.count
	}
	return int(sum) / t.hashCount
}

// Empty returns true if the IBLT is empty.
func (t *KTable) Empty() bool {
	for _, bucket := range t.buckets {
		if bucket.count != 0 || bucket.idSum != 0 || bucket.hashSum != 0 {
			return false
		}
	}
	return true
}

// CompatibleWith returns true if the two IBLTs have the same parameters (hashCount and bucketCount).
func (t *KTable) CompatibleWith(other *KTable) bool {
	return t.hashCount == other.hashCount && len(t.buckets) == len(other.buckets)
}

// Subtract subtracts the given IBLT from this one.
// This function panics if the two IBLTs are not compatible.
func (t *KTable) Subtract(other *KTable) {
	if !t.CompatibleWith(other) {
		panic("IBLTs have different parameters")
	}
	for i := range t.buckets {
		t.buckets[i].idSum ^= other.buckets[i].idSum
		t.buckets[i].hashSum ^= other.buckets[i].hashSum
		t.buckets[i].count -= other.buckets[i].count
	}
}

// Peel returns a sequence of (key, count) pairs.
// This function removes the key from the IBLT, so consider Copy() if you want to keep the original IBLT.
// The count is 1 if the key is present in the IBLT, -1 if it is missing (following a Subtract).
// This allows doing set reconciliation with two IBLTs (A and B), A.Subtract(B), then A.Peel():
// - a count of 1 means that the key is in A, but not in B
// - a count of -1 means that the key is in B, but not in A
// IMPORTANT: the peeling process can fail if the IBLT is saturated. This function, however, will NOT return an error in
// that case. Instead, you NEED to call Empty() to check if the process completed successfully.
func (t *KTable) Peel() iter.Seq2[uint64, int64] {
	return func(yield func(uint64, int64) bool) {
		queue := make([]int, 0)

		// initialize the queue with all pure bucket indices
		for idx := 0; idx < len(t.buckets); idx++ {
			if t.isPure(idx) {
				queue = append(queue, idx)
			}
		}

		for len(queue) > 0 {
			// pop the first element
			idx := queue[0]
			queue = queue[1:]

			if t.buckets[idx].count == 0 {
				continue // already processed
			}

			key := t.buckets[idx].idSum
			count := t.buckets[idx].count
			if !yield(key, count) {
				return
			}

			// Delete and find the new pure buckets
			h, indices := t.indices(key)
			for idx := range indices {
				t.buckets[idx].idSum ^= key
				t.buckets[idx].hashSum ^= h
				t.buckets[idx].count -= count

				if t.isPure(idx) {
					queue = append(queue, idx)
				}
			}
		}
	}
}

// PeelHas is the same as Peel, but only returns keys that are present in the IBLT (count==1).
func (t *KTable) PeelHas() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		for key, count := range t.Peel() {
			if count == 1 {
				if !yield(key) {
					return
				}
			}
		}
	}
}

// PeelMisses is the same as Peel, but only returns keys that are missing from the IBLT (count==-1).
func (t *KTable) PeelMisses() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		for key, count := range t.Peel() {
			if count == -1 {
				if !yield(key) {
					return
				}
			}
		}
	}
}

func (t *KTable) isPure(idx int) bool {
	count := t.buckets[idx].count
	if count != 1 && count != -1 {
		return false
	}
	// we have a single key, does the hash matches?
	return splitmix64(t.buckets[idx].idSum) == t.buckets[idx].hashSum
}

// indices return the t.hashCount indices for the given hash.
func (t *KTable) indices(key uint64) (uint64, iter.Seq[int]) {
	// We want to generate t.HashCount hashes for the given key. Different strategies exist for this.
	// One way would be to use the PRNG version of splitmix64, and use the key as the seed. This would give us
	// a series of random numbers, which we can then use as indices. A variant (which we will use here) is to
	// compute one hash with splitmix64, then do a "double hashing" to get the indices. Essentially, this will
	// apply a simpler permutation to the key, which is faster than using a PRNG for similar characteristic.
	// This is almost 2x faster.

	// first, take a hash of the key
	h := splitmix64(key)

	// double hashing param
	delta := h >> 33
	delta |= 1 // ensure odd, to avoid short cycles (common with power of two moduli)

	return h, func(yield func(int) bool) {
		for i := 0; i < t.hashCount; i++ {
			idx := int((h + uint64(i)*delta) % uint64(len(t.buckets)))
			if !yield(idx) {
				return
			}
		}
	}
}

// splitmix64 is a hash function that is fast and simple.
// However, it might not be resistant to attack where an adversary tries, for example, to force more keys into the same
// bucket, which could prevent peeling.
// Ref: https://xorshift.di.unimi.it/splitmix64.c
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
