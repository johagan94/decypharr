package usenet

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sourcegraph/conc/pool"
)

// segmentResult holds a fetched segment and its index for ordered writing
type segmentResult struct {
	index int
	data  []byte
	err   error
}

// ProgressCallback is called periodically during download with progress info
// downloaded: total bytes written so far, speed: bytes per second (estimated)
type ProgressCallback func(downloaded int64, speed int64)

// Download downloads a file by fetching segments in parallel and streaming to writer in order.
// Bytes flow to the writer progressively as in-order segments complete - no waiting for all segments.
// If progressCallback is provided, it will be called after each segment write with current progress.
func (u *Usenet) Download(ctx context.Context, nzoID, filename string, writer io.Writer, progressCallback ProgressCallback) error {
	// get file metadata
	file, err := u.getFile(nzoID, filename)
	if err != nil {
		return fmt.Errorf("failed to get file: %w", err)
	}

	if len(file.Segments) == 0 {
		return fmt.Errorf("file has no segments: %s", file.Name)
	}

	// Track progress
	var completedSegments atomic.Int64
	var downloadedBytes atomic.Int64

	// Channel for segment results - buffered to allow parallel fetching ahead
	resultChan := make(chan segmentResult, u.maxConnections*2)

	// Map to hold out-of-order segments waiting to be written
	pendingSegments := make(map[int][]byte)
	var pendingMu sync.Mutex
	nextToWrite := 0

	// Error tracking
	var writeErr error
	var writeErrMu sync.Mutex

	// Writer goroutine - writes segments in order as they arrive
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for result := range resultChan {
			if result.err != nil {
				writeErrMu.Lock()
				if writeErr == nil {
					writeErr = result.err
				}
				writeErrMu.Unlock()
				continue
			}

			pendingMu.Lock()
			pendingSegments[result.index] = result.data

			// Write all consecutive segments starting from nextToWrite
			for {
				data, exists := pendingSegments[nextToWrite]
				if !exists {
					break
				}
				delete(pendingSegments, nextToWrite)
				pendingMu.Unlock()

				// Write to output
				n, err := writer.Write(data)
				if err != nil {
					writeErrMu.Lock()
					if writeErr == nil {
						writeErr = fmt.Errorf("write failed at segment %d: %w", nextToWrite, err)
					}
					writeErrMu.Unlock()
					pendingMu.Lock()
					break
				}

				completedSegments.Add(1)
				downloaded := downloadedBytes.Add(int64(n))
				nextToWrite++

				// Call progress callback if provided
				if progressCallback != nil {
					// Estimate speed (rough: assume ~1s per segment batch)
					completed := completedSegments.Load()
					speed := downloaded / max(1, completed) * int64(u.maxConnections)
					progressCallback(downloaded, speed)
				}

				pendingMu.Lock()
			}
			pendingMu.Unlock()
		}
	}()

	// Fetch segments in pipelined batches. Each goroutine acquires one NNTP
	// connection and sends a full batch of BODY commands before reading any
	// response, amortising per-segment RTT across pipelineBatchSize segments.
	const pipelineBatchSize = 50

	type segBatch struct {
		startIdx int
		segs     []storage.NZBSegment
	}
	var batches []segBatch
	for i := 0; i < len(file.Segments); i += pipelineBatchSize {
		end := i + pipelineBatchSize
		if end > len(file.Segments) {
			end = len(file.Segments)
		}
		batches = append(batches, segBatch{startIdx: i, segs: file.Segments[i:end]})
	}

	p := pool.New().WithContext(ctx).WithMaxGoroutines(max(u.maxConnections, 1))

	for _, batch := range batches {
		b := batch
		p.Go(func(ctx context.Context) error {
			writeErrMu.Lock()
			if writeErr != nil {
				writeErrMu.Unlock()
				return writeErr
			}
			writeErrMu.Unlock()

			if ctx.Err() != nil {
				return ctx.Err()
			}

			// Build message-ID slice for this batch.
			msgIDs := make([]string, len(b.segs))
			for i, s := range b.segs {
				msgIDs[i] = s.MessageID
			}

			// Pipeline all BODY commands in one round-trip.
			results, err := u.nntp.ExecuteBatch(ctx, msgIDs)
			if err != nil && results == nil {
				// Total connection failure for the batch.
				for i, s := range b.segs {
					resultChan <- segmentResult{
						index: b.startIdx + i,
						err:   fmt.Errorf("segment %d (%s): %w", b.startIdx+i, s.MessageID, err),
					}
				}
				return nil
			}

			for i, result := range results {
				segIdx := b.startIdx + i
				seg := b.segs[i]  // storage.NZBSegment

				if result.Error != nil {
					resultChan <- segmentResult{index: segIdx, err: fmt.Errorf("segment %d: %w", segIdx, result.Error)}
					continue
				}

				data := result.Data
				if seg.SegmentDataStart > 0 {
					if seg.SegmentDataStart >= int64(len(data)) {
						resultChan <- segmentResult{index: segIdx, err: fmt.Errorf("segment %d: offset exceeds data", segIdx)}
						continue
					}
					data = data[seg.SegmentDataStart:]
				}
				if int64(len(data)) > seg.Bytes {
					data = data[:seg.Bytes]
				}
				resultChan <- segmentResult{index: segIdx, data: data}
			}
			return nil
		})
	}

	// Wait for all fetches to complete, then close result channel
	fetchErr := p.Wait()
	close(resultChan)

	// Wait for writer to finish
	writerWg.Wait()

	// Check for errors
	if writeErr != nil {
		return writeErr
	}
	if fetchErr != nil {
		return fetchErr
	}

	u.logger.Info().
		Str("file", filename).
		Int64("bytes", downloadedBytes.Load()).
		Msg("Download complete")

	return nil
}
