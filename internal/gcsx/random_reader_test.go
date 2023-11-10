// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcsx

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/googlecloudplatform/gcsfuse/internal/cache/file"
	"github.com/googlecloudplatform/gcsfuse/internal/cache/file/downloader"
	"github.com/googlecloudplatform/gcsfuse/internal/cache/lru"
	"github.com/googlecloudplatform/gcsfuse/internal/cache/util"
	"github.com/googlecloudplatform/gcsfuse/internal/storage"
	"github.com/googlecloudplatform/gcsfuse/internal/storage/gcs"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/oglemock"
	. "github.com/jacobsa/ogletest"
	"golang.org/x/net/context"
)

func TestRandomReader(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Invariant-checking random reader
////////////////////////////////////////////////////////////////////////

type checkingRandomReader struct {
	ctx     context.Context
	wrapped *randomReader
}

func (rr *checkingRandomReader) ReadAt(p []byte, offset int64) (int, bool, error) {
	rr.wrapped.CheckInvariants()
	defer rr.wrapped.CheckInvariants()
	return rr.wrapped.ReadAt(rr.ctx, p, offset)
}

func (rr *checkingRandomReader) Destroy() {
	rr.wrapped.CheckInvariants()
	rr.wrapped.Destroy()
}

////////////////////////////////////////////////////////////////////////
// Counting closer
////////////////////////////////////////////////////////////////////////

type countingCloser struct {
	io.Reader
	closeCount int
}

func (cc *countingCloser) Close() (err error) {
	cc.closeCount++
	return
}

////////////////////////////////////////////////////////////////////////
// Blocking reader
////////////////////////////////////////////////////////////////////////

// A reader that blocks until a channel is closed, then returns an error.
type blockingReader struct {
	c chan struct{}
}

func (br *blockingReader) Read(p []byte) (n int, err error) {
	<-br.c
	err = errors.New("blockingReader")
	return
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func rangeStartIs(expected uint64) (m Matcher) {
	pred := func(c interface{}) (err error) {
		req := c.(*gcs.ReadObjectRequest)
		if req.Range == nil {
			err = errors.New("which has a nil range")
			return
		}

		if req.Range.Start != expected {
			err = fmt.Errorf("which has Start == %d", req.Range.Start)
			return
		}

		return
	}

	m = NewMatcher(pred, fmt.Sprintf("has range start %d", expected))
	return
}

func rangeLimitIs(expected uint64) (m Matcher) {
	pred := func(c interface{}) (err error) {
		req := c.(*gcs.ReadObjectRequest)
		if req.Range == nil {
			err = errors.New("which has a nil range")
			return
		}

		if req.Range.Limit != expected {
			err = fmt.Errorf("which has Limit == %d", req.Range.Limit)
			return
		}

		return
	}

	m = NewMatcher(pred, fmt.Sprintf("has range limit %d", expected))
	return
}

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

const sequentialReadSizeInMb = 10
const sequentialReadSizeInBytes = sequentialReadSizeInMb * MB
const CacheMaxSize = 20 * MB

type RandomReaderTest struct {
	object        *gcs.MinObject
	bucket        storage.MockBucket
	rr            checkingRandomReader
	cacheLocation string
	jobManager    *downloader.JobManager
	cacheHandler  *file.CacheHandler
}

func init() { RegisterTestSuite(&RandomReaderTest{}) }

var _ SetUpInterface = &RandomReaderTest{}
var _ TearDownInterface = &RandomReaderTest{}

func (t *RandomReaderTest) SetUp(ti *TestInfo) {
	t.rr.ctx = ti.Ctx

	// Manufacture an object record.
	t.object = &gcs.MinObject{
		Name:       "foo",
		Size:       17,
		Generation: 1234,
	}

	// Create the bucket.
	t.bucket = storage.NewMockBucket(ti.MockController, "bucket")

	t.cacheLocation = path.Join(os.Getenv("HOME"), "cache/location")
	lruCache := lru.NewCache(CacheMaxSize)
	t.jobManager = downloader.NewJobManager(lruCache, util.DefaultFilePerm, t.cacheLocation, sequentialReadSizeInMb)
	t.cacheHandler = file.NewCacheHandler(lruCache, t.jobManager, t.cacheLocation, util.DefaultFilePerm)

	// Set up the reader.
	rr := NewRandomReader(t.object, t.bucket, sequentialReadSizeInMb, nil, false)
	t.rr.wrapped = rr.(*randomReader)
}

func (t *RandomReaderTest) TearDown() {
	t.rr.Destroy()
}

func getRandomContent(objectSize int) []byte {
	testContent := make([]byte, objectSize)
	_, err := rand.Read(testContent)
	AssertEq(nil, err)

	return testContent
}
func getReadCloser(content []byte) io.ReadCloser {
	r := bytes.NewReader(content)
	rc := io.NopCloser(r)
	return rc
}

////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////

func (t *RandomReaderTest) EmptyRead() {
	// Nothing should happen.
	buf := make([]byte, 0)

	n, _, err := t.rr.ReadAt(buf, 0)

	ExpectEq(0, n)
	ExpectEq(nil, err)
}

func (t *RandomReaderTest) ReadAtEndOfObject() {
	buf := make([]byte, 1)

	n, _, err := t.rr.ReadAt(buf, int64(t.object.Size))

	ExpectEq(0, n)
	ExpectEq(io.EOF, err)
}

func (t *RandomReaderTest) ReadPastEndOfObject() {
	buf := make([]byte, 1)

	n, cacheHit, err := t.rr.ReadAt(buf, int64(t.object.Size)+1)

	ExpectFalse(cacheHit)
	ExpectEq(0, n)
	ExpectEq(io.EOF, err)
}

func (t *RandomReaderTest) NoExistingReader() {
	// The bucket should be called to set up a new reader.
	ExpectCall(t.bucket, "NewReader")(Any(), Any()).
		WillOnce(Return(nil, errors.New("")))
	buf := make([]byte, 1)

	_, _, err := t.rr.ReadAt(buf, 0)

	AssertNe(nil, err)
}

func (t *RandomReaderTest) ExistingReader_WrongOffset() {
	// Simulate an existing reader.
	t.rr.wrapped.reader = ioutil.NopCloser(strings.NewReader("xxx"))
	t.rr.wrapped.cancel = func() {}
	t.rr.wrapped.start = 2
	t.rr.wrapped.limit = 5
	// The bucket should be called to set up a new reader.
	ExpectCall(t.bucket, "NewReader")(Any(), Any()).
		WillOnce(Return(nil, errors.New("")))
	buf := make([]byte, 1)

	_, _, err := t.rr.ReadAt(buf, 0)

	AssertNe(nil, err)
}

func (t *RandomReaderTest) NewReaderReturnsError() {
	ExpectCall(t.bucket, "NewReader")(Any(), Any()).
		WillOnce(Return(nil, errors.New("taco")))
	buf := make([]byte, 1)

	_, _, err := t.rr.ReadAt(buf, 0)

	ExpectThat(err, Error(HasSubstr("NewReader")))
	ExpectThat(err, Error(HasSubstr("taco")))
}

func (t *RandomReaderTest) ReaderFails() {
	// Bucket
	r := iotest.OneByteReader(iotest.TimeoutReader(strings.NewReader("xxx")))
	rc := ioutil.NopCloser(r)

	ExpectCall(t.bucket, "NewReader")(Any(), Any()).
		WillOnce(Return(rc, nil))

	// Call
	buf := make([]byte, 3)
	_, _, err := t.rr.ReadAt(buf, 0)

	ExpectThat(err, Error(HasSubstr("readFull")))
	ExpectThat(err, Error(HasSubstr(iotest.ErrTimeout.Error())))
}

func (t *RandomReaderTest) ReaderOvershootsRange() {
	// Simulate a reader that is supposed to return two more bytes, but actually
	// returns three when asked to.
	t.rr.wrapped.reader = ioutil.NopCloser(strings.NewReader("xxx"))
	t.rr.wrapped.cancel = func() {}
	t.rr.wrapped.start = 0
	t.rr.wrapped.limit = 2

	// Try to read three bytes.
	buf := make([]byte, 3)
	_, _, err := t.rr.ReadAt(buf, 0)

	ExpectThat(err, Error(HasSubstr("1 too many bytes")))
}

func (t *RandomReaderTest) ReaderNotExhausted() {
	// Set up a reader that has three bytes left to give.
	rc := &countingCloser{
		Reader: strings.NewReader("abc"),
	}

	t.rr.wrapped.reader = rc
	t.rr.wrapped.cancel = func() {}
	t.rr.wrapped.start = 1
	t.rr.wrapped.limit = 4

	// Read two bytes.
	buf := make([]byte, 2)
	n, cacheHit, err := t.rr.ReadAt(buf, 1)

	ExpectFalse(cacheHit)
	ExpectEq(2, n)
	ExpectEq(nil, err)
	ExpectEq("ab", string(buf[:n]))

	ExpectEq(0, rc.closeCount)
	ExpectEq(rc, t.rr.wrapped.reader)
	ExpectEq(3, t.rr.wrapped.start)
	ExpectEq(4, t.rr.wrapped.limit)
}

func (t *RandomReaderTest) ReaderExhausted_ReadFinished() {
	// Set up a reader that has three bytes left to give.
	rc := &countingCloser{
		Reader: strings.NewReader("abc"),
	}

	t.rr.wrapped.reader = rc
	t.rr.wrapped.cancel = func() {}
	t.rr.wrapped.start = 1
	t.rr.wrapped.limit = 4

	// Read three bytes.
	buf := make([]byte, 3)
	n, cacheHit, err := t.rr.ReadAt(buf, 1)

	ExpectFalse(cacheHit)
	ExpectEq(3, n)
	ExpectEq(nil, err)
	ExpectEq("abc", string(buf[:n]))

	ExpectEq(1, rc.closeCount)
	ExpectEq(nil, t.rr.wrapped.reader)
	ExpectEq(nil, t.rr.wrapped.cancel)
	ExpectEq(4, t.rr.wrapped.limit)
}

func (t *RandomReaderTest) ReaderExhausted_ReadNotFinished() {
	// Set up a reader that has three bytes left to give.
	rc := &countingCloser{
		Reader: strings.NewReader("abc"),
	}

	t.rr.wrapped.reader = rc
	t.rr.wrapped.cancel = func() {}
	t.rr.wrapped.start = 1
	t.rr.wrapped.limit = 4

	// The bucket should be called at the previous limit to obtain a new reader.
	ExpectCall(t.bucket, "NewReader")(Any(), rangeStartIs(4)).
		WillOnce(Return(nil, errors.New("")))

	// Attempt to read four bytes.
	buf := make([]byte, 4)
	n, cacheHit, _ := t.rr.ReadAt(buf, 1)

	ExpectFalse(cacheHit)
	AssertGe(n, 3)
	ExpectEq("abc", string(buf[:3]))

	ExpectEq(1, rc.closeCount)
	ExpectEq(nil, t.rr.wrapped.reader)
	ExpectEq(nil, t.rr.wrapped.cancel)
	ExpectEq(4, t.rr.wrapped.limit)
}

func (t *RandomReaderTest) PropagatesCancellation() {
	// Set up a reader that will block until we tell it to return.
	finishRead := make(chan struct{})
	rc := ioutil.NopCloser(&blockingReader{finishRead})

	t.rr.wrapped.reader = rc
	t.rr.wrapped.start = 1
	t.rr.wrapped.limit = 4

	// Snoop on when cancel is called.
	cancelCalled := make(chan struct{})
	t.rr.wrapped.cancel = func() { close(cancelCalled) }

	// Start a read in the background using a context that we control. It should
	// not yet return.
	readReturned := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		buf := make([]byte, 2)
		t.rr.wrapped.ReadAt(ctx, buf, 1)
		close(readReturned)
	}()

	select {
	case <-time.After(10 * time.Millisecond):
	case <-readReturned:
		AddFailure("Read returned early.")
		AbortTest()
	}

	// When we cancel our context, the random reader should cancel the read
	// context.
	cancel()
	<-cancelCalled

	// Clean up.
	close(finishRead)
	<-readReturned
}

func (t *RandomReaderTest) DoesntPropagateCancellationAfterReturning() {
	// Set up a reader that will return three bytes.
	t.rr.wrapped.reader = ioutil.NopCloser(strings.NewReader("xxx"))
	t.rr.wrapped.start = 1
	t.rr.wrapped.limit = 4

	// Snoop on when cancel is called.
	cancelCalled := make(chan struct{})
	t.rr.wrapped.cancel = func() { close(cancelCalled) }

	// Successfully read two bytes using a context whose cancellation we control.
	ctx, cancel := context.WithCancel(context.Background())
	buf := make([]byte, 2)
	n, cacheHit, err := t.rr.wrapped.ReadAt(ctx, buf, 1)

	ExpectFalse(cacheHit)
	AssertEq(nil, err)
	AssertEq(2, n)

	// If we cancel the calling context now, it should not cause the underlying
	// read context to be cancelled.
	cancel()
	select {
	case <-time.After(10 * time.Millisecond):
	case <-cancelCalled:
		AddFailure("Read context unexpectedly cancelled.")
		AbortTest()
	}
}

func (t *RandomReaderTest) UpgradesReadsToObjectSize() {
	const objectSize = 2 * MB
	t.object.Size = objectSize

	const readSize = 10
	AssertLt(readSize, objectSize)

	// Simulate an existing reader at a mismatched offset.
	t.rr.wrapped.reader = ioutil.NopCloser(strings.NewReader("xxx"))
	t.rr.wrapped.cancel = func() {}
	t.rr.wrapped.start = 2
	t.rr.wrapped.limit = 5

	// The bucket should be asked to read the entire object, even though we only
	// ask for readSize bytes below, to minimize the cost for GCS requests.
	r := strings.NewReader(strings.Repeat("x", objectSize))
	rc := ioutil.NopCloser(r)

	ExpectCall(t.bucket, "NewReader")(
		Any(),
		AllOf(rangeStartIs(1), rangeLimitIs(objectSize))).
		WillOnce(Return(rc, nil))

	// Call through.
	buf := make([]byte, readSize)
	_, cacheHit, err := t.rr.ReadAt(buf, 1)

	// Check the state now.
	ExpectFalse(cacheHit)
	ExpectEq(nil, err)
	ExpectEq(1+readSize, t.rr.wrapped.start)
	ExpectEq(objectSize, t.rr.wrapped.limit)
}

func (t *RandomReaderTest) UpgradeReadsToAverageSize() {
	t.object.Size = 1 << 40
	const totalReadBytes = 6 * MB
	const numReads = 2
	const avgReadBytes = totalReadBytes / numReads

	const expectedBytesToRead = avgReadBytes
	const start = 1
	const readSize = 2 * minReadSize

	// Simulate an existing reader at a mismatched offset.
	t.rr.wrapped.seeks = numReads
	t.rr.wrapped.totalReadBytes = totalReadBytes
	t.rr.wrapped.reader = ioutil.NopCloser(strings.NewReader("xxx"))
	t.rr.wrapped.cancel = func() {}
	t.rr.wrapped.start = 2
	t.rr.wrapped.limit = 5

	// The bucket should be asked to read expectedBytesToRead bytes.
	r := strings.NewReader(strings.Repeat("x", expectedBytesToRead))
	rc := ioutil.NopCloser(r)

	ExpectCall(t.bucket, "NewReader")(
		Any(),
		AllOf(
			rangeStartIs(start),
			rangeLimitIs(start+expectedBytesToRead),
		)).WillOnce(Return(rc, nil))

	// Call through.
	buf := make([]byte, readSize)
	_, cacheHit, err := t.rr.ReadAt(buf, start)

	// Check the state now.
	ExpectFalse(cacheHit)
	AssertEq(nil, err)
	ExpectEq(start+expectedBytesToRead, t.rr.wrapped.limit)
}

func (t *RandomReaderTest) UpgradesSequentialReads_ExistingReader() {
	t.object.Size = 1 << 40
	const readSize = 10

	// Simulate an existing reader at the correct offset, which will be exhausted
	// by the read below.
	const existingSize = 3
	r := strings.NewReader(strings.Repeat("x", existingSize))

	t.rr.wrapped.reader = ioutil.NopCloser(r)
	t.rr.wrapped.cancel = func() {}
	t.rr.wrapped.start = 1
	t.rr.wrapped.limit = 1 + existingSize

	// The bucket should be asked to read up to the end of the object.
	r = strings.NewReader(strings.Repeat("x", readSize-existingSize))
	rc := ioutil.NopCloser(r)

	ExpectCall(t.bucket, "NewReader")(
		Any(),
		AllOf(rangeStartIs(1+existingSize), rangeLimitIs(1+existingSize+sequentialReadSizeInBytes))).
		WillOnce(Return(rc, nil))

	// Call through.
	buf := make([]byte, readSize)
	_, cacheHit, err := t.rr.ReadAt(buf, 1)

	// Check the state now.
	ExpectFalse(cacheHit)
	AssertEq(nil, err)
	ExpectEq(1+readSize, t.rr.wrapped.start)
	// Limit is same as the byteRange of last GCS call made.
	ExpectEq(1+existingSize+sequentialReadSizeInBytes, t.rr.wrapped.limit)
}

func (t *RandomReaderTest) UpgradesSequentialReads_NoExistingReader() {
	t.object.Size = 1 << 40
	const readSize = 1 * MB
	// Set up the custom randomReader.
	rr := NewRandomReader(t.object, t.bucket, readSize/MB, nil, false)
	t.rr.wrapped = rr.(*randomReader)

	// Simulate a previous exhausted reader that ended at the offset from which
	// we read below.
	t.rr.wrapped.start = 1
	t.rr.wrapped.limit = 1

	// The bucket should be asked to read up to the end of the object.
	r := strings.NewReader(strings.Repeat("x", readSize))
	rc := ioutil.NopCloser(r)

	ExpectCall(t.bucket, "NewReader")(
		Any(),
		AllOf(rangeStartIs(1), rangeLimitIs(1+readSize))).
		WillOnce(Return(rc, nil))

	// Call through.
	buf := make([]byte, readSize)
	_, cacheHit, err := t.rr.ReadAt(buf, 1)

	// Check the state now.
	ExpectFalse(cacheHit)
	ExpectEq(nil, err)
	ExpectEq(1+readSize, t.rr.wrapped.start)
	ExpectEq(1+readSize, t.rr.wrapped.limit)
}

func (t *RandomReaderTest) SequentialReads_NoExistingReader_requestedSizeGreaterThanChunkSize() {
	t.object.Size = 1 << 40
	const chunkSize = 1 * MB
	const readSize = 3 * MB
	// Set up the custom randomReader.
	rr := NewRandomReader(t.object, t.bucket, chunkSize/MB, nil, false)
	t.rr.wrapped = rr.(*randomReader)
	// Create readers for each chunk.
	chunk1Reader := strings.NewReader(strings.Repeat("x", chunkSize))
	chunk1RC := ioutil.NopCloser(chunk1Reader)
	chunk2Reader := strings.NewReader(strings.Repeat("x", chunkSize))
	chunk2RC := ioutil.NopCloser(chunk2Reader)
	chunk3Reader := strings.NewReader(strings.Repeat("x", chunkSize))
	chunk3RC := ioutil.NopCloser(chunk3Reader)
	// Mock the NewReader calls to return chunkReaders created above.
	// We will make 3 GCS calls to satisfy the requested read size. But since we
	// already have a reader with 'existingSize' data, we will first read that data
	// and then make GCS calls. So call sequence is
	//  [0, chunkSize) -> newReader
	//  [hunkSize, chunkSize*2) -> newReader
	//  [chunkSize*2, chunkSize*3) -> newReader
	ExpectCall(t.bucket, "NewReader")(
		Any(),
		AllOf(rangeStartIs(0), rangeLimitIs(chunkSize))).
		WillOnce(Return(chunk1RC, nil))
	ExpectCall(t.bucket, "NewReader")(
		Any(),
		AllOf(rangeStartIs(chunkSize), rangeLimitIs(chunkSize*2))).
		WillOnce(Return(chunk2RC, nil))
	ExpectCall(t.bucket, "NewReader")(
		Any(),
		AllOf(rangeStartIs(chunkSize*2), rangeLimitIs(chunkSize*3))).
		WillOnce(Return(chunk3RC, nil))

	// Call through.
	buf := make([]byte, readSize)
	_, cacheHit, err := t.rr.ReadAt(buf, 0)

	// Check the state now.
	ExpectFalse(cacheHit)
	ExpectEq(nil, err)
	// Start is the total data read.
	ExpectEq(readSize, t.rr.wrapped.start)
	// Limit is same as the byteRange of last GCS call made.
	ExpectEq(readSize, t.rr.wrapped.limit)
}

func (t *RandomReaderTest) SequentialReads_existingReader_requestedSizeGreaterThanChunkSize() {
	t.object.Size = 1 << 40
	const chunkSize = 1 * MB
	const readSize = 3 * MB
	// Set up the custom randomReader.
	rr := NewRandomReader(t.object, t.bucket, chunkSize/MB, nil, false)
	t.rr.wrapped = rr.(*randomReader)
	// Simulate an existing reader at the correct offset, which will be exhausted
	// by the read below.
	const existingSize = 3
	r := strings.NewReader(strings.Repeat("x", existingSize))
	t.rr.wrapped.reader = ioutil.NopCloser(r)
	t.rr.wrapped.cancel = func() {}
	t.rr.wrapped.start = 0
	t.rr.wrapped.limit = existingSize
	// Create readers for each chunk.
	chunk1Reader := strings.NewReader(strings.Repeat("x", chunkSize))
	chunk1RC := ioutil.NopCloser(chunk1Reader)
	chunk2Reader := strings.NewReader(strings.Repeat("x", chunkSize))
	chunk2RC := ioutil.NopCloser(chunk2Reader)
	chunk3Reader := strings.NewReader(strings.Repeat("x", chunkSize))
	chunk3RC := ioutil.NopCloser(chunk3Reader)
	// Mock the NewReader calls to return chunkReaders created above.
	// We will make 3 GCS calls to satisfy the requested read size. But since we
	// already have a reader with 'existingSize' data, we will first read that data
	// and then make GCS calls. So call sequence is
	//  [0, existingSize) -> existing reader
	//  [existingSize, existingSize+chunkSize) -> newReader
	//  [existingSize+chunkSize, existingSize+chunkSize*2) -> newReader
	//  [existingSize+chunkSize*2, existingSize+chunkSize*3) -> newReader
	ExpectCall(t.bucket, "NewReader")(
		Any(),
		AllOf(rangeStartIs(existingSize), rangeLimitIs(existingSize+chunkSize))).
		WillOnce(Return(chunk1RC, nil))
	ExpectCall(t.bucket, "NewReader")(
		Any(),
		AllOf(rangeStartIs(existingSize+chunkSize), rangeLimitIs(existingSize+chunkSize*2))).
		WillOnce(Return(chunk2RC, nil))
	ExpectCall(t.bucket, "NewReader")(
		Any(),
		AllOf(rangeStartIs(existingSize+chunkSize*2), rangeLimitIs(existingSize+chunkSize*3))).
		WillOnce(Return(chunk3RC, nil))

	// Call through.
	buf := make([]byte, readSize)
	_, cacheHit, err := t.rr.ReadAt(buf, 0)

	// Check the state now.
	ExpectFalse(cacheHit)
	ExpectEq(nil, err)
	// Start is the total data read.
	ExpectEq(readSize, t.rr.wrapped.start)
	// Limit is same as the byteRange of last GCS call made.
	ExpectEq(existingSize+readSize, t.rr.wrapped.limit)
}

func (t *RandomReaderTest) Test_ReadAt_CacheHitFullObject() {
	t.rr.wrapped.fileCacheHandler = t.cacheHandler
	objectSize := t.object.Size
	testContent := getRandomContent(int(objectSize))
	rc := getReadCloser(testContent)
	ExpectCall(t.bucket, "NewReader")(
		Any(),
		AllOf(rangeStartIs(0), rangeLimitIs(objectSize))).
		WillRepeatedly(Return(rc, nil))
	ExpectCall(t.bucket, "Name")().
		WillRepeatedly(Return("test"))

	buf := make([]byte, objectSize)
	_, cacheHit, err := t.rr.ReadAt(buf, 0)

	ExpectTrue(cacheHit)
	ExpectEq(nil, err)
	ExpectTrue(reflect.DeepEqual(testContent, buf))
}

func (t *RandomReaderTest) Test_ReadAt_CacheHitRangeRead() {
	t.rr.wrapped.fileCacheHandler = t.cacheHandler
	objectSize := t.object.Size
	testContent := getRandomContent(int(objectSize))
	rc := getReadCloser(testContent)
	ExpectCall(t.bucket, "NewReader")(
		Any(),
		AllOf(rangeStartIs(0), rangeLimitIs(objectSize))).
		WillRepeatedly(Return(rc, nil))
	ExpectCall(t.bucket, "Name")().
		WillRepeatedly(Return("test"))
	start := 0
	end := 10 // not included
	AssertLt(end, objectSize)

	buf := make([]byte, end-start)
	_, cacheHit, err := t.rr.ReadAt(buf, int64(start))

	ExpectTrue(cacheHit)
	ExpectEq(nil, err)
	ExpectTrue(reflect.DeepEqual(testContent[start:end], buf))
}

func (t *RandomReaderTest) Test_ReadAt_CacheMissRandomReadNotStartWithZeroOffset() {
	t.rr.wrapped.fileCacheHandler = t.cacheHandler
	objectSize := t.object.Size
	testContent := getRandomContent(int(objectSize))
	rc := getReadCloser(testContent)
	ExpectCall(t.bucket, "NewReader")(
		Any(),
		AllOf(rangeStartIs(0), rangeLimitIs(objectSize))).
		WillRepeatedly(Return(rc, nil))
	ExpectCall(t.bucket, "Name")().
		WillRepeatedly(Return("test"))
	start := 5
	end := 10 // not included
	AssertLt(end, objectSize)

	buf := make([]byte, end-start)
	_, cacheHit, err := t.rr.ReadAt(buf, int64(start))

	ExpectFalse(cacheHit)
	ExpectNe(nil, err)
}

//func (t *RandomReaderTest) Test_ReadAt_CacheMissDueToInvalidJob() {
//	t.rr.wrapped.fileCacheHandler = t.cacheHandler
//	objectSize := t.object.Size
//	r1 := strings.NewReader(strings.Repeat("x", int(objectSize)))
//	rc1 := ioutil.NopCloser(r1)
//	ExpectCall(t.bucket, "NewReader")(
//		Any(),
//		AllOf(rangeStartIs(0), rangeLimitIs(objectSize))).
//		WillRepeatedly(Return(rc1, nil))
//	ExpectCall(t.bucket, "Name")().
//		WillRepeatedly(Return("test"))
//	buf := make([]byte, objectSize)
//	_, err := t.rr.ReadAt(buf, 0)
//	ExpectEq(nil, err)
//	expectedContent := []byte(strings.Repeat("x", int(objectSize)))
//	ExpectTrue(reflect.DeepEqual(expectedContent, buf))
//	ExpectNe(nil, t.rr.wrapped.fileCacheHandler)
//	job := t.jobManager.GetJob(t.object, t.bucket)
//	jobStatus := job.GetStatus()
//	ExpectEq(jobStatus.Name, downloader.COMPLETED)
//	err = t.rr.wrapped.fileCacheHandler.InvalidateCache(t.object, t.bucket)
//	AssertEq(nil, err)
//	r2 := strings.NewReader(strings.Repeat("x", int(objectSize)))
//	rc2 := ioutil.NopCloser(r2)
//	// Second reader (rc2) is required, since first reader (rc) is completely read.
//	// Reading again will return EOF.
//	ExpectCall(t.bucket, "NewReader")(
//		Any(),
//		AllOf(rangeStartIs(0), rangeLimitIs(objectSize))).
//		WillRepeatedly(Return(rc2, nil))
//
//	_, err = t.rr.ReadAt(buf, 0)
//
//	ExpectEq(nil, err)
//	ExpectTrue(reflect.DeepEqual(expectedContent, buf))
//	ExpectEq(nil, t.rr.wrapped.fileCacheHandle)
//}
//
//func (t *RandomReaderTest) Test_readFromCache_SuccessfulRead() {
//	t.rr.wrapped.fileCacheHandler = t.cacheHandler
//	objectSize := t.object.Size
//	r := strings.NewReader(strings.Repeat("x", int(objectSize)))
//	rc := ioutil.NopCloser(r)
//	ExpectCall(t.bucket, "NewReader")(
//		Any(),
//		AllOf(rangeStartIs(0), rangeLimitIs(objectSize))).
//		WillRepeatedly(Return(rc, nil))
//	ExpectCall(t.bucket, "Name")().
//		WillRepeatedly(Return("test"))
//	buf := make([]byte, objectSize)
//
//	_, err := t.rr.wrapped.readFromCache(t.rr.ctx, buf, 0)
//
//	ExpectEq(nil, err)
//	expectedContent := []byte(strings.Repeat("x", int(objectSize)))
//	ExpectTrue(reflect.DeepEqual(expectedContent, buf))
//	ExpectNe(nil, t.rr.wrapped.fileCacheHandle)
//	job := t.jobManager.GetJob(t.object, t.bucket)
//	jobStatus := job.GetStatus()
//	ExpectEq(jobStatus.Name, downloader.COMPLETED)
//}
//
//func (t *RandomReaderTest) Test_readFromCache_InvalidJob() {
//	t.rr.wrapped.fileCacheHandler = t.cacheHandler
//	objectSize := t.object.Size
//	r := strings.NewReader(strings.Repeat("x", int(objectSize)))
//	rc := ioutil.NopCloser(r)
//	ExpectCall(t.bucket, "NewReader")(
//		Any(),
//		AllOf(rangeStartIs(0), rangeLimitIs(objectSize))).
//		WillRepeatedly(Return(rc, nil))
//	ExpectCall(t.bucket, "Name")().
//		WillRepeatedly(Return("test"))
//	buf := make([]byte, objectSize)
//	_, err := t.rr.wrapped.readFromCache(t.rr.ctx, buf, 0)
//	ExpectEq(nil, err)
//	expectedContent := []byte(strings.Repeat("x", int(objectSize)))
//	ExpectTrue(reflect.DeepEqual(expectedContent, buf))
//	ExpectNe(nil, t.rr.wrapped.fileCacheHandle)
//	job := t.jobManager.GetJob(t.object, t.bucket)
//	jobStatus := job.GetStatus()
//	ExpectEq(jobStatus.Name, downloader.COMPLETED)
//	err = t.rr.wrapped.fileCacheHandler.InvalidateCache(t.object, t.bucket)
//	ExpectEq(nil, err)
//
//	// Since job is invalid, hence it will fail with util.InvalidFileDownloadJobErrMsg
//	_, err = t.rr.wrapped.readFromCache(t.rr.ctx, buf, 0)
//
//	ExpectNe(nil, err)
//	ExpectTrue(strings.Contains(err.Error(), util.InvalidFileDownloadJobErrMsg))
//}
//
//func (t *RandomReaderTest) Test_readFromCache_RandomReadWithRandomDownloadTrue() {
//	t.rr.wrapped.fileCacheHandler = t.cacheHandler
//	t.rr.wrapped.downloadForRandomRead = true
//	objectSize := t.object.Size
//	r := strings.NewReader(strings.Repeat("x", int(objectSize)))
//	rc := ioutil.NopCloser(r)
//	ExpectCall(t.bucket, "NewReader")(
//		Any(),
//		AllOf(rangeStartIs(0), rangeLimitIs(objectSize))).
//		WillRepeatedly(Return(rc, nil))
//	ExpectCall(t.bucket, "Name")().
//		WillRepeatedly(Return("test"))
//	buf := make([]byte, objectSize)
//
//	// Read from offset = 1 will trigger the random read.
//	_, err := t.rr.wrapped.readFromCache(t.rr.ctx, buf, 1)
//
//	ExpectNe(nil, err)
//	ExpectTrue(strings.Contains(err.Error(), util.FallbackToGCSErrMsg))
//	ExpectNe(nil, t.rr.wrapped.fileCacheHandle)
//	job := t.jobManager.GetJob(t.object, t.bucket)
//	jobStatus := job.GetStatus()
//	ExpectTrue(jobStatus.Name == downloader.DOWNLOADING || jobStatus.Name == downloader.COMPLETED)
//}
//
//func (t *RandomReaderTest) Test_readFromCache_RandomReadWithRandomDownloadFalse() {
//	t.rr.wrapped.fileCacheHandler = t.cacheHandler
//	t.rr.wrapped.downloadForRandomRead = false
//	objectSize := t.object.Size
//	r := strings.NewReader(strings.Repeat("x", int(objectSize)))
//	rc := ioutil.NopCloser(r)
//	ExpectCall(t.bucket, "NewReader")(
//		Any(),
//		AllOf(rangeStartIs(0), rangeLimitIs(objectSize))).
//		WillRepeatedly(Return(rc, nil))
//	ExpectCall(t.bucket, "Name")().
//		WillRepeatedly(Return("test"))
//	buf := make([]byte, objectSize)
//
//	// Read from offset - 1 will trigger the random read.
//	_, err := t.rr.wrapped.readFromCache(t.rr.ctx, buf, 1)
//
//	ExpectNe(nil, err)
//	ExpectTrue(strings.Contains(err.Error(), util.FallbackToGCSErrMsg))
//	ExpectNe(nil, t.rr.wrapped.fileCacheHandle)
//	job := t.jobManager.GetJob(t.object, t.bucket)
//	jobStatus := job.GetStatus()
//	ExpectTrue(jobStatus.Name == downloader.NOT_STARTED)
//}
