package iblt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
)

// KVTable is an Invertible Bloom Lookup Table, which holds keys and values.
// It's a probabilistic concise data structure for set representation that supports a peeling operation as the recovery of the elements in the represented set.
// It can be used in particular for set reconciliation, see the example.
// KVTable is NOT thread safe.
type KVTable struct {
	buckets     []kvBucket
	hashCount   int // number of hashes applied to each key
	valueMaxLen int
}

type kvBucket struct {
	keySum   uint64 // bitwise XOR of all keys in the bucket
	hashSum  uint64 // bitwise XOR of all hashes of keys in the bucket
	count    int64  // number of elements in the bucket
	valueSum []byte // bitwise XOR of all values in the bucket
}

// NewKVTable creates a new key-only IBLT with the given parameters.
// - hashCount parameter is the number of hashes applied to each key.
// - bucketCount parameter is the number of buckets.
// - valueMaxLen parameter is the maximum length in byte of the stored values. Be aware that all buckets will accommodate for that maximum size.
// Those parameters directly affect the memory usage of the IBLT, as well as the probability to succeed in a peeling operation.
func NewKVTable(bucketCount, hashCount, valueMaxLen int) *KVTable {
	res := &KVTable{
		buckets:     make([]kvBucket, bucketCount),
		hashCount:   hashCount,
		valueMaxLen: valueMaxLen,
	}
	for i := range res.buckets {
		res.buckets[i].valueSum = make([]byte, valueMaxLen)
	}
	return res
}

func KVTableFromBytes(data []byte) (*KVTable, error) {
	if len(data) < 2+2+2 {
		return nil, fmt.Errorf("not enough data for header: %d", len(data))
	}
	hashCount := int(binary.BigEndian.Uint16(data[0:2]))
	bucketCount := int(binary.BigEndian.Uint16(data[2:4]))
	valueMaxLen := int(binary.BigEndian.Uint16(data[4:6]))
	data = data[6:]

	res := NewKVTable(bucketCount, hashCount, valueMaxLen)

	bucketSize := 24 + valueMaxLen
	if len(data) < bucketCount*bucketSize {
		return nil, fmt.Errorf("not enough data for buckets: %d", len(data))
	}

	for i := 0; i < bucketCount; i++ {
		res.buckets[i].keySum = binary.BigEndian.Uint64(data[0:8])
		res.buckets[i].hashSum = binary.BigEndian.Uint64(data[8:16])
		res.buckets[i].count = int64(binary.BigEndian.Uint64(data[16:24]))
		res.buckets[i].valueSum = make([]byte, valueMaxLen)
		copy(res.buckets[i].valueSum, data[24:24+valueMaxLen])
		data = data[bucketSize:]
	}
	return res, nil
}

func KVTableFromReader(r io.Reader) (*KVTable, error) {
	var header [6]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("not enough data for header: %w", err)
	}
	hashCount := int(binary.BigEndian.Uint16(header[0:2]))
	bucketCount := int(binary.BigEndian.Uint16(header[2:4]))
	valueMaxLen := int(binary.BigEndian.Uint16(header[4:6]))

	res := NewKVTable(bucketCount, hashCount, valueMaxLen)

	var buf [24]byte
	for i := 0; i < bucketCount; i++ {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return nil, fmt.Errorf("not enough data for buckets: %w", err)
		}
		res.buckets[i].keySum = binary.BigEndian.Uint64(buf[0:8])
		res.buckets[i].hashSum = binary.BigEndian.Uint64(buf[8:16])
		res.buckets[i].count = int64(binary.BigEndian.Uint64(buf[16:24]))
		res.buckets[i].valueSum = make([]byte, valueMaxLen)
		if _, err := io.ReadFull(r, res.buckets[i].valueSum); err != nil {
			return nil, fmt.Errorf("not enough data for buckets: %w", err)
		}
	}
	return res, nil
}

// ToBytes returns the serialized representation of the key-only IBLT.
func (t *KVTable) ToBytes() []byte {
	data := make([]byte, 0, 2+2+2+len(t.buckets)*(3*8+t.valueMaxLen))
	data = binary.BigEndian.AppendUint16(data, uint16(t.hashCount))
	data = binary.BigEndian.AppendUint16(data, uint16(len(t.buckets)))
	data = binary.BigEndian.AppendUint16(data, uint16(t.valueMaxLen))
	for _, bucket := range t.buckets {
		data = binary.BigEndian.AppendUint64(data, bucket.keySum)
		data = binary.BigEndian.AppendUint64(data, bucket.hashSum)
		data = binary.BigEndian.AppendUint64(data, uint64(bucket.count))
		data = append(data, bucket.valueSum...)
	}
	return data
}

// ToWriter serializes the IBLT to the given writer.
func (t *KVTable) ToWriter(w io.Writer) error {
	var header [6]byte
	binary.BigEndian.PutUint16(header[0:2], uint16(t.hashCount))
	binary.BigEndian.PutUint16(header[2:4], uint16(len(t.buckets)))
	binary.BigEndian.PutUint16(header[4:6], uint16(t.valueMaxLen))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	var buf [24]byte
	for _, bucket := range t.buckets {
		binary.BigEndian.PutUint64(buf[0:8], bucket.keySum)
		binary.BigEndian.PutUint64(buf[8:16], bucket.hashSum)
		binary.BigEndian.PutUint64(buf[16:24], uint64(bucket.count))
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
		if _, err := w.Write(bucket.valueSum); err != nil {
			return err
		}
	}
	return nil
}

// Copy returns a deep copy of the IBLT.
func (t *KVTable) Copy() *KVTable {
	res := &KVTable{
		buckets:     make([]kvBucket, len(t.buckets)),
		hashCount:   t.hashCount,
		valueMaxLen: t.valueMaxLen,
	}
	for i := range t.buckets {
		res.buckets[i].keySum = t.buckets[i].keySum
		res.buckets[i].hashSum = t.buckets[i].hashSum
		res.buckets[i].count = t.buckets[i].count
		res.buckets[i].valueSum = make([]byte, t.valueMaxLen)
		copy(res.buckets[i].valueSum, t.buckets[i].valueSum)
	}
	return res
}

// Equals returns true if the two IBLTs are identical.
func (t *KVTable) Equals(other *KVTable) bool {
	if !t.CompatibleWith(other) {
		return false
	}
	for i := range t.buckets {
		if t.buckets[i].keySum != other.buckets[i].keySum {
			return false
		}
		if t.buckets[i].hashSum != other.buckets[i].hashSum {
			return false
		}
		if t.buckets[i].count != other.buckets[i].count {
			return false
		}
		if !bytes.Equal(t.buckets[i].valueSum, other.buckets[i].valueSum) {
			return false
		}
	}
	return true
}

// Insert adds the given key/value to the IBLT.
func (t *KVTable) Insert(key uint64, value []byte) {
	if len(value) > t.valueMaxLen {
		panic("value larger than valueMaxLen")
	}
	h, indices := t.indices(key)
	for idx := range indices {
		t.buckets[idx].keySum ^= key
		t.buckets[idx].hashSum ^= h
		t.buckets[idx].count++
		for i := 0; i < len(value); i++ {
			t.buckets[idx].valueSum[i] ^= value[i]
		}
	}
}

// Delete removes the given key/value from the IBLT.
func (t *KVTable) Delete(key uint64, value []byte) {
	h, indices := t.indices(key)
	for idx := range indices {
		t.buckets[idx].keySum ^= key
		t.buckets[idx].hashSum ^= h
		t.buckets[idx].count--
		for i := 0; i < len(value); i++ {
			t.buckets[idx].valueSum[i] ^= value[i]
		}
	}
}

// Count returns the number of keys in the IBLT.
// IMPORTANT: this number is only accurate if:
// - no Subtract has been done
// - no Delete with a key not in the IBLT has been done
func (t *KVTable) Count() int {
	var sum int64
	for _, bucket := range t.buckets {
		sum += bucket.count
	}
	return int(sum) / t.hashCount
}

// Empty returns true if the IBLT is empty.
func (t *KVTable) Empty() bool {
	for _, bucket := range t.buckets {
		if bucket.count != 0 || bucket.keySum != 0 || bucket.hashSum != 0 {
			return false
		}
	}
	return true
}

// CompatibleWith returns true if the two IBLTs have the same parameters (hashCount and bucketCount).
func (t *KVTable) CompatibleWith(other *KVTable) bool {
	return t.hashCount == other.hashCount && len(t.buckets) == len(other.buckets) && t.valueMaxLen == other.valueMaxLen
}

// Subtract subtracts the given IBLT from this one.
// This function panics if the two IBLTs are not compatible.
func (t *KVTable) Subtract(other *KVTable) {
	if !t.CompatibleWith(other) {
		panic("IBLTs have different parameters")
	}
	for i := range t.buckets {
		t.buckets[i].keySum ^= other.buckets[i].keySum
		t.buckets[i].hashSum ^= other.buckets[i].hashSum
		t.buckets[i].count -= other.buckets[i].count
		for j := 0; j < t.valueMaxLen; j++ {
			t.buckets[i].valueSum[j] ^= other.buckets[i].valueSum[j]
		}
	}
}

type KV struct {
	Key   uint64
	Value []byte
}

// Peel returns a sequence of ([key,value], count) pairs.
// This function removes the key from the IBLT, so consider Copy() if you want to keep the original IBLT.
// The count is 1 if the key is present in the IBLT, -1 if it is missing (following a Subtract).
// This allows doing set reconciliation with two IBLTs (A and B), A.Subtract(B), then A.Peel():
// - a count of 1 means that the key is in A, but not in B
// - a count of -1 means that the key is in B, but not in A
// IMPORTANT: the peeling process can fail if the IBLT is saturated. This function, however, will NOT return an error in
// that case. Instead, you NEED to call Empty() to check if the process completed successfully.
// IMPORTANT: the IBLT doesn't conserve the value length, so the returned values will always be of length t.valueMaxLen.
func (t *KVTable) Peel() iter.Seq2[KV, int64] {
	return func(yield func(KV, int64) bool) {
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

			key := t.buckets[idx].keySum
			count := t.buckets[idx].count
			value := t.buckets[idx].valueSum
			if !yield(KV{Key: key, Value: value}, count) {
				return
			}

			// Delete and find the new pure buckets
			h, indices := t.indices(key)
			for idx := range indices {
				t.buckets[idx].keySum ^= key
				t.buckets[idx].hashSum ^= h
				t.buckets[idx].count -= count

				if t.isPure(idx) {
					queue = append(queue, idx)
				}
			}
		}
	}
}

// PeelHas is the same as Peel, but only returns key/values that are present in the IBLT (count==1).
func (t *KVTable) PeelHas() iter.Seq2[uint64, []byte] {
	return func(yield func(uint64, []byte) bool) {
		for kv, count := range t.Peel() {
			if count == 1 {
				if !yield(kv.Key, kv.Value) {
					return
				}
			}
		}
	}
}

// PeelMisses is the same as Peel, but only returns key/values that are missing from the IBLT (count==-1).
func (t *KVTable) PeelMisses() iter.Seq2[uint64, []byte] {
	return func(yield func(uint64, []byte) bool) {
		for kv, count := range t.Peel() {
			if count == -1 {
				if !yield(kv.Key, kv.Value) {
					return
				}
			}
		}
	}
}

func (t *KVTable) isPure(idx int) bool {
	count := t.buckets[idx].count
	if count != 1 && count != -1 {
		return false
	}
	// we have a single key, does the hash match?
	return splitmix64(t.buckets[idx].keySum) == t.buckets[idx].hashSum
}

// indices return the t.hashCount indices for the given hash.
func (t *KVTable) indices(key uint64) (uint64, iter.Seq[int]) {
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
