package iblt

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func value(i uint64, len int) []byte {
	return bytes.Repeat([]byte{byte(i)}, len)
}

func TestKVSerial(t *testing.T) {
	const valueLen = 10
	table1 := NewKVTable(100, 4, valueLen)
	for i := uint64(0); i < 200_000; i++ {
		table1.Insert(i, value(i, valueLen))
	}
	table2 := NewKVTable(100, 4, valueLen)
	for i := uint64(5); i < 200_005; i++ {
		table2.Insert(i, value(i, valueLen))
	}
	// This makes sure we have negative counts in the table
	table1.Subtract(table2)

	// ToBytes/FromBytes
	data := table1.ToBytes()
	fromBytes, err := KVTableFromBytes(data)
	require.NoError(t, err)
	require.True(t, fromBytes.Equals(table1))

	// ToWriter/FromReader
	var buf bytes.Buffer
	require.NoError(t, table1.ToWriter(&buf))
	fromReader, err := KVTableFromReader(&buf)
	require.NoError(t, err)
	require.True(t, fromReader.Equals(table1))
}

func BenchmarkKVSerial(b *testing.B) {
	const valueLen = 10
	table1 := NewKVTable(100, 4, valueLen)
	for i := uint64(0); i < 200_000; i++ {
		table1.Insert(i, value(i, valueLen))
	}
	data := table1.ToBytes()

	b.ResetTimer()

	b.Run("ToBytes", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = table1.ToBytes()
		}
	})

	b.Run("FromBytes", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = KVTableFromBytes(data)
		}
	})

	b.Run("ToWriter", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = table1.ToWriter(io.Discard)
		}
	})

	b.Run("FromReader", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = KVTableFromReader(bytes.NewReader(data))
		}
	})
}

func TestKVCopy(t *testing.T) {
	const valueLen = 10
	table1 := NewKVTable(100, 4, valueLen)
	for i := uint64(0); i < 200_000; i++ {
		table1.Insert(i, value(i, valueLen))
	}
	table2 := table1.Copy()
	require.True(t, table1.Equals(table2))
}

func TestKVEquals(t *testing.T) {
	const valueLen = 10
	table1 := NewKVTable(100, 4, valueLen)
	for i := uint64(0); i < 200_000; i++ {
		table1.Insert(i, value(i, valueLen))
	}
	table2 := NewKVTable(100, 4, valueLen)
	for i := uint64(0); i < 200_000; i++ {
		table2.Insert(i, value(i, valueLen))
	}
	require.True(t, table1.Equals(table2))

	table3 := NewKVTable(100, 3, valueLen) // different hashCount
	for i := uint64(0); i < 200_000; i++ {
		table3.Insert(i, value(i, valueLen))
	}
	require.False(t, table1.Equals(table3))

	table4 := NewKVTable(101, 4, valueLen) // different bucketCount
	for i := uint64(0); i < 200_000; i++ {
		table4.Insert(i, value(i, valueLen))
	}
	require.False(t, table1.Equals(table4))

	table5 := NewKVTable(101, 4, 11) // different valueMaxLen
	for i := uint64(0); i < 200_000; i++ {
		table5.Insert(i, value(i, valueLen))
	}
	require.False(t, table1.Equals(table5))

	table6 := NewKVTable(100, 4, valueLen) // one more element
	for i := uint64(0); i < 200_001; i++ {
		table6.Insert(i, value(i, valueLen))
	}
	require.False(t, table1.Equals(table6))
}

func TestKVInsertDelete(t *testing.T) {
	const count = 1000
	const valueLen = 10

	table := NewKVTable(100, 4, valueLen)

	for i := uint64(0); i < count; i++ {
		table.Insert(i, value(i, valueLen))
	}
	require.False(t, table.Empty())

	for i := uint64(0); i < count; i++ {
		table.Delete(i, value(i, valueLen))
	}
	require.True(t, table.Empty())
}

func BenchmarkKVInsert(b *testing.B) {
	const valueLen = 10
	val := value(0, valueLen)
	table := NewKVTable(100, 4, valueLen)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		table.Insert(uint64(i), val)
	}
}

func BenchmarkKVDelete(b *testing.B) {
	const valueLen = 10
	val := value(0, valueLen)
	table := NewKVTable(100, 4, valueLen)
	for i := uint64(0); i < 1000; i++ {
		table.Insert(i, val)
	}
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// that will delete elements that are not present, but that's OK
		table.Delete(uint64(i), val)
	}
}

func TestKVEmpty(t *testing.T) {
	const valueLen = 10
	table := NewKVTable(100, 4, valueLen)
	require.True(t, table.Empty())

	table.Insert(0, value(0, valueLen))
	require.False(t, table.Empty())
	table.Delete(0, value(0, valueLen))
	require.True(t, table.Empty())
}

func TestKVPeel(t *testing.T) {
	const valueLen = 10
	const inserts = 25 // low enough to be able to peel

	table := NewKVTable(100, 4, valueLen)

	var elements []KV
	for i := uint64(0); i < inserts; i++ {
		val := value(i, valueLen)
		table.Insert(i, val)
		elements = append(elements, KV{Key: i, Value: val})
	}

	var res []KV
	for k, v := range table.PeelHas() {
		res = append(res, KV{Key: k, Value: v})
	}

	require.ElementsMatch(t, elements, res)
	require.True(t, table.Empty())
}

func BenchmarkKVPeel(b *testing.B) {
	const valueLen = 10
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		table := NewKVTable(100, 4, valueLen)
		for j := uint64(0); j < 25; j++ {
			table.Insert(j, value(j, valueLen))
		}
		b.StartTimer()

		for range table.Peel() {
			// do nothing
		}
	}
}

func TestKVSetReconciliation(t *testing.T) {
	const valueLen = 10
	table1 := NewKVTable(100, 4, valueLen)
	for i := uint64(0); i < 200_000; i++ {
		table1.Insert(i, value(i, valueLen))
	}

	table2 := NewKVTable(100, 4, valueLen)
	for i := uint64(5); i < 200_005; i++ {
		table2.Insert(i, value(i, valueLen))
	}

	table1.Subtract(table2)

	var has []KV
	for k, v := range table1.Copy().PeelHas() {
		has = append(has, KV{Key: k, Value: v})
	}
	var misses []KV
	for k, v := range table1.Copy().PeelMisses() {
		misses = append(misses, KV{Key: k, Value: v})
	}

	require.ElementsMatch(t, has, []KV{
		{Key: 0, Value: value(0, valueLen)},
		{Key: 1, Value: value(1, valueLen)},
		{Key: 2, Value: value(2, valueLen)},
		{Key: 3, Value: value(3, valueLen)},
		{Key: 4, Value: value(4, valueLen)},
	})
	require.ElementsMatch(t, misses, []KV{
		{Key: 200_000, Value: value(200_000, valueLen)},
		{Key: 200_001, Value: value(200_001, valueLen)},
		{Key: 200_002, Value: value(200_002, valueLen)},
		{Key: 200_003, Value: value(200_003, valueLen)},
		{Key: 200_004, Value: value(200_004, valueLen)},
	})

	type element struct {
		Key   uint64
		Value []byte
		Count int64
	}
	var elements []element
	for kv, count := range table1.Copy().Peel() {
		elements = append(elements, element{Key: kv.Key, Value: kv.Value, Count: count})
	}

	require.ElementsMatch(t, elements, []element{
		{Key: 0, Value: value(0, valueLen), Count: 1},
		{Key: 1, Value: value(1, valueLen), Count: 1},
		{Key: 2, Value: value(2, valueLen), Count: 1},
		{Key: 3, Value: value(3, valueLen), Count: 1},
		{Key: 4, Value: value(4, valueLen), Count: 1},
		{Key: 200_000, Value: value(200_000, valueLen), Count: -1},
		{Key: 200_001, Value: value(200_001, valueLen), Count: -1},
		{Key: 200_002, Value: value(200_002, valueLen), Count: -1},
		{Key: 200_003, Value: value(200_003, valueLen), Count: -1},
		{Key: 200_004, Value: value(200_004, valueLen), Count: -1},
	})
}
