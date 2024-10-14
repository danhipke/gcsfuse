package storage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/googlecloudplatform/gcsfuse/v2/internal/logger"
)

// TODO: memory usage is much higher than expected - remove unnecessary memory allocations (e.g. file free up buffers once we no longer need them)
// TODO: there is no cap across gcsfuse on how much memory this can take up - multiple filehandles will each cache the same amount of data. look at restricting readahead buffers, but perhaps this is not important for diskless environments with sufficient memory.
// TODO: determine optimal # of workers. currently, 16 and 10MB seems good, but maybe 5MB is sufficient?
// TODO: add tests.

// ParallelReader is a reader capable of downloading from GCS in parallel.
type ParallelReader struct {
	ctx             context.Context
	cancel          context.CancelFunc
	start           int64
	end             int64
	curOffset       int64
	partSizeBytes   int64
	workers         []*downloadWorker
	currentWorkerID int
	idleWorkersIDs  chan int
}

// downloadWorker will buffer and try downloading a single part.
type downloadWorker struct {
	object       *storage.ObjectHandle
	reader       io.Reader
	buffer       []byte
	chunkSize    int64
	bufferOffset int64
	BytesRemain  int64
	copyReady    bool
	last         bool
	errReading   error
}

// NewParallelReader creates workers with readers for parallel download.
func NewParallelReader(ctx context.Context, bucket *storage.BucketHandle, objectName string, readCompressed bool, start, generation, end int64, chunkSizeMb, numWorkers int32) (*ParallelReader, error) {

	r := &ParallelReader{
		start:          start,
		end:            int64(end),
		curOffset:      start,
		partSizeBytes:  int64(chunkSizeMb * 1024 * 1024),
		workers:        make([]*downloadWorker, numWorkers),
		idleWorkersIDs: make(chan int, numWorkers),
	}
	r.ctx, r.cancel = context.WithCancel(ctx)

	logger.Debugf("Performing parallel read - objectName: %s, numWorkers: %d, start: %d, end: %d", objectName, numWorkers, start, end)
	for i := 0; i < int(numWorkers); i++ {
		r.workers[i] = &downloadWorker{
			buffer: make([]byte, r.partSizeBytes),
		}
		// Creates a new bucket handle for each worker to support parallel downloads.
		r.workers[i].object = bucket.Object(objectName)
		if readCompressed {
			r.workers[i].object = r.workers[i].object.ReadCompressed(true)
		}
		if generation != 0 {
			r.workers[i].object = r.workers[i].object.Generation(generation)
		}
	}
	r.start = start
	r.curOffset = start
	return r, nil
}

// Read asynchronously downloads data and returns the properly ordered bytes.
func (r *ParallelReader) Read(p []byte) (int, error) {
	if r == nil {
		return 0, errors.New("no parallel reader defined")
	}
	if r.curOffset >= r.end || (r.workers[r.currentWorkerID].last && r.workers[r.currentWorkerID].BytesRemain == 0) {
		return 0, io.EOF
	}
	// In the first Read() call, kickoff all the workers to download their respective first chunk in parallel.
	if r.curOffset == r.start {
		logger.Debugf("Workers started")
		for i := 0; i < len(r.workers); i++ {
			if startByte := r.start + r.partSizeBytes*int64(i); startByte < r.end {
				if r.workers[i] == nil {
					r.cancel()
					return 0, fmt.Errorf("no worker defined")
				}
				assignWorkersChunk(r, startByte, i)
			}
		}
	}
	// Wait for the worker to be ready which is downloading the required chunk.
	for r.workers[r.currentWorkerID].copyReady == false {
		id := <-r.idleWorkersIDs
		select {
		case <-r.ctx.Done():
			logger.Info("Parallel restore cancellation requested")
			return 0, r.ctx.Err()
		default:
			// If the worker fails to read from GCS, we must exit the loop.
			if r.workers[id].errReading != nil {
				logger.Errorf("Failed to read from GCS: %v", r.workers[id].errReading)
				r.cancel()
				return 0, r.workers[id].errReading
			}
			r.workers[id].copyReady = true
		}
	}
	// Copy data from the worker's buffer to the provided p buffer.
	id := r.currentWorkerID
	bufOffset := r.workers[id].bufferOffset

	// Copy valid data, avoiding issues with buffer overruns.
	dataToCopy := r.workers[id].buffer[bufOffset:r.workers[id].chunkSize]
	n := copy(p, dataToCopy)

	r.curOffset += int64(n)
	r.workers[id].bufferOffset += int64(n)
	r.workers[id].BytesRemain -= int64(n)

	// Reset and assign the next chunk to the worker only if the entire buffer has been consumed.
	if r.workers[id].BytesRemain == 0 {
		r.workers[id].copyReady = false
		// Check if the current worker need to be assigned a new chunk.
		// The next len(r.workers) - 1 chunks are already assigned to the workers for downloading.
		if startByte := r.curOffset + int64(len(r.workers)-1)*r.partSizeBytes; startByte < r.end {
			assignWorkersChunk(r, startByte, id)
		} else {
			r.workers[id].last = true
		}

		// Update the currentWorkerID to the next worker's ID having the next chunk.
		if r.currentWorkerID++; r.currentWorkerID == len(r.workers) {
			r.currentWorkerID = 0
		}
	}
	return n, nil
}

func assignWorkersChunk(r *ParallelReader, startByte int64, id int) {
	// The buffer is filled in parallel using go routines.
	go func() {
		r.workers[id].errReading = fillWorkerBuffer(r, r.workers[id], startByte)
		r.idleWorkersIDs <- id //	Pass the worker's ID to the channel once the worker's buffer is filled.
	}()
}

// fillWorkerBuffer fills the worker's buffer with data from GCS.
func fillWorkerBuffer(r *ParallelReader, worker *downloadWorker, startByte int64) error {
	// Assigns reader to worker with the given startByte and chunk length.
	var err error
	toRead := min(r.partSizeBytes, r.end-startByte)
	if worker.reader, err = worker.object.NewRangeReader(r.ctx, startByte, toRead /*r.partSizeBytes*/); err != nil {
		return fmt.Errorf("failed to create range reader: %v", err)
	}

	// Reads data into the worker's buffer until the whole length is read.
	bytesRead := int64(0)
	for {
		select {
		case <-r.ctx.Done():
			return fmt.Errorf("context cancellation called")
		default:
			var n int
			if n, err = worker.reader.Read(worker.buffer[bytesRead:]); err != nil && err != io.EOF {
				return fmt.Errorf("failed to read from range reader: %v", err)
			}
			if n == 0 || err == io.EOF {
				// When all bytes are read, the loop will exit.
				// TODO: overwriting here seems wrong?
				bytesRead = toRead
				worker.BytesRemain = bytesRead
				worker.chunkSize = bytesRead
				worker.bufferOffset = 0
				return nil
			}
			bytesRead += int64(n)
		}
	}
}

// Close cancels any in progress transfers and performs any necessary clean up.
func (r *ParallelReader) Close() error {
	r.cancel()
	return nil
}
