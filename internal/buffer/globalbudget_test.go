package buffer

import (
	"bytes"
	"testing"
)

// TestGlobalRAMBudgetCapsAndNoLeak verifies that the process-wide RAM ceiling
// bounds the SUM of resident bytes across many buffers (the 13 GiB blow-up was
// hundreds of streams each under their own 32 MB cap), that data is still
// served correctly when blocks fall back to disk, and that Close releases a
// buffer's resident bytes from the global accounting (no leak).
func TestGlobalRAMBudgetCapsAndNoLeak(t *testing.T) {
	savedLimit := globalRAMLimit
	globalRAMBytes.Store(0)
	globalRAMLimit = 8 * blockSize // 8 MiB global ceiling
	t.Cleanup(func() {
		globalRAMLimit = savedLimit
		globalRAMBytes.Store(0)
	})

	const nbuf = 5
	const perBuf = 4 // 4 MiB each; 5×4 = 20 MiB desired, well over the 8 MiB ceiling

	bufs := make([]*Buffer, nbuf)
	for i := range bufs {
		b, err := New(Config{
			MemorySize: perBuf * blockSize,
			TotalSize:  int64(perBuf) * blockSize,
		})
		if err != nil {
			t.Fatalf("New[%d]: %v", i, err)
		}
		bufs[i] = b
	}

	val := func(bi, blk int) byte { return byte(bi*perBuf + blk + 1) }

	for bi, b := range bufs {
		for blk := 0; blk < perBuf; blk++ {
			data := bytes.Repeat([]byte{val(bi, blk)}, blockSize)
			if _, err := b.WriteAt(data, int64(blk)*blockSize); err != nil {
				t.Fatalf("WriteAt buf %d blk %d: %v", bi, blk, err)
			}
		}
	}

	// Invariant: total RAM across all buffers never exceeds the global ceiling.
	if got := GlobalRAMBytes(); got > globalRAMLimit {
		t.Fatalf("global RAM %d B exceeded ceiling %d B", got, globalRAMLimit)
	}

	// All data must still read back correctly — blocks that could not be
	// RAM-cached fell back to the disk-backing file.
	for bi, b := range bufs {
		for blk := 0; blk < perBuf; blk++ {
			got := make([]byte, blockSize)
			if _, err := b.ReadAt(got, int64(blk)*blockSize); err != nil {
				t.Fatalf("ReadAt buf %d blk %d: %v", bi, blk, err)
			}
			if got[0] != val(bi, blk) || got[blockSize-1] != val(bi, blk) {
				t.Fatalf("buf %d blk %d: data mismatch got %d want %d", bi, blk, got[0], val(bi, blk))
			}
		}
	}

	// Async read-promotes must also respect the ceiling.
	if got := GlobalRAMBytes(); got > globalRAMLimit {
		t.Fatalf("global RAM %d B exceeded ceiling %d B after reads", got, globalRAMLimit)
	}

	// Closing every buffer must return the global counter to zero. Close drops
	// resident blocks without routing through dropBlockLocked, so it must
	// release their bytes from the global accounting itself.
	for i, b := range bufs {
		if err := b.Close(); err != nil {
			t.Fatalf("Close[%d]: %v", i, err)
		}
	}
	if got := GlobalRAMBytes(); got != 0 {
		t.Fatalf("global RAM leaked after closing all buffers: %d B (want 0)", got)
	}
}

// TestGlobalRAMUnlimitedWhenZero confirms a limit of 0 disables the ceiling.
func TestGlobalRAMUnlimitedWhenZero(t *testing.T) {
	savedLimit := globalRAMLimit
	savedBytes := globalRAMBytes.Load()
	globalRAMLimit = 0
	globalRAMBytes.Store(savedLimit + (1 << 40)) // pretend RAM is huge
	t.Cleanup(func() {
		globalRAMLimit = savedLimit
		globalRAMBytes.Store(savedBytes)
	})
	if !globalRAMHasRoom() {
		t.Fatal("globalRAMHasRoom must be true when limit is 0 (unlimited)")
	}
}
