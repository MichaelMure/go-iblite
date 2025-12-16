package iblt

import (
	"bytes"
	"io"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKSerial(t *testing.T) {
	table1 := NewKTable(100, 4)
	for i := uint64(0); i < 200_000; i++ {
		table1.Insert(i)
	}
	table2 := NewKTable(100, 4)
	for i := uint64(5); i < 200_005; i++ {
		table2.Insert(i)
	}
	// This makes sure we have negative counts in the table
	table1.Subtract(table2)

	// ToBytes/FromBytes
	data := table1.ToBytes()
	fromBytes, err := KTableFromBytes(data)
	require.NoError(t, err)
	require.True(t, fromBytes.Equals(table1))

	// ToWriter/FromReader
	var buf bytes.Buffer
	require.NoError(t, table1.ToWriter(&buf))
	fromReader, err := KTableFromReader(&buf)
	require.NoError(t, err)
	require.True(t, fromReader.Equals(table1))
}

func BenchmarkKSerial(b *testing.B) {
	table1 := NewKTable(100, 4)
	for i := uint64(0); i < 200_000; i++ {
		table1.Insert(i)
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
			_, _ = KTableFromBytes(data)
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
			_, _ = KTableFromReader(bytes.NewReader(data))
		}
	})
}

func TestKCopy(t *testing.T) {
	table1 := NewKTable(100, 4)
	for i := uint64(0); i < 200_000; i++ {
		table1.Insert(i)
	}
	table2 := table1.Copy()
	require.True(t, table1.Equals(table2))
}

func TestKEquals(t *testing.T) {
	table1 := NewKTable(100, 4)
	for i := uint64(0); i < 200_000; i++ {
		table1.Insert(i)
	}
	table2 := NewKTable(100, 4)
	for i := uint64(0); i < 200_000; i++ {
		table2.Insert(i)
	}
	require.True(t, table1.Equals(table2))

	table3 := NewKTable(100, 3) // different hashCount
	for i := uint64(0); i < 200_000; i++ {
		table3.Insert(i)
	}
	require.False(t, table1.Equals(table3))

	table4 := NewKTable(101, 4) // different bucketCount
	for i := uint64(0); i < 200_000; i++ {
		table4.Insert(i)
	}
	require.False(t, table1.Equals(table4))

	table5 := NewKTable(100, 4) // one more element
	for i := uint64(0); i < 200_001; i++ {
		table5.Insert(i)
	}
	require.False(t, table1.Equals(table5))
}

func TestKInsertDelete(t *testing.T) {
	const count = 1000

	table := NewKTable(100, 4)

	for i := uint64(0); i < count; i++ {
		table.Insert(i)
	}
	require.False(t, table.Empty())

	for i := uint64(0); i < count; i++ {
		table.Delete(i)
	}
	require.True(t, table.Empty())
}

func BenchmarkKInsert(b *testing.B) {
	table := NewKTable(100, 4)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		table.Insert(uint64(i))
	}
}

func BenchmarkKDelete(b *testing.B) {
	table := NewKTable(100, 4)
	for i := uint64(0); i < 1000; i++ {
		table.Insert(i)
	}
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// that will delete elements that are not present, but that's OK
		table.Delete(uint64(i))
	}
}

func TestKEmpty(t *testing.T) {
	table := NewKTable(100, 4)
	require.True(t, table.Empty())

	table.Insert(0)
	require.False(t, table.Empty())
	table.Delete(0)
	require.True(t, table.Empty())
}

func TestKPeel(t *testing.T) {
	const inserts = 25 // low enough to be able to peel

	table := NewKTable(100, 4)

	var elements []uint64
	for i := uint64(0); i < inserts; i++ {
		table.Insert(i)
		elements = append(elements, i)
	}

	require.ElementsMatch(t, elements, slices.Collect(table.PeelHas()))
	require.True(t, table.Empty())
}

func BenchmarkKPeel(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		table := NewKTable(100, 4)
		for j := uint64(0); j < 25; j++ {
			table.Insert(j)
		}
		b.StartTimer()

		for range table.Peel() {
			// do nothing
		}
	}
}

func TestKSetReconciliation(t *testing.T) {
	table1 := NewKTable(100, 4)
	for i := uint64(0); i < 200_000; i++ {
		table1.Insert(i)
	}

	table2 := NewKTable(100, 4)
	for i := uint64(5); i < 200_005; i++ {
		table2.Insert(i)
	}

	table1.Subtract(table2)

	require.ElementsMatch(t, slices.Collect(table1.Copy().PeelHas()), []uint64{0, 1, 2, 3, 4})
	require.ElementsMatch(t, slices.Collect(table1.Copy().PeelMisses()), []uint64{200_000, 200_001, 200_002, 200_003, 200_004})

	type element struct {
		Key   uint64
		Count int64
	}
	var elements []element
	for key, count := range table1.Copy().Peel() {
		elements = append(elements, element{Key: key, Count: count})
	}
	require.ElementsMatch(t, elements, []element{
		{Key: 0, Count: 1}, {Key: 1, Count: 1}, {Key: 2, Count: 1}, {Key: 3, Count: 1}, {Key: 4, Count: 1},
		{Key: 200_000, Count: -1}, {Key: 200_001, Count: -1}, {Key: 200_002, Count: -1}, {Key: 200_003, Count: -1}, {Key: 200_004, Count: -1},
	})
}
