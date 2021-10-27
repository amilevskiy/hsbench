// hsbench.go
// Copyright (c) 2017 Wasabi Technology, Inc.
// Copyright (c) 2019 Red Hat Inc.

package main

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const modeString = "cxiplgd"

// Global variables
var accessKey, secretSey, urlHost, bucketPrefix, bucketList, objectPrefix, region, modes, output, jsonOutput, sizeArg string
var buckets []string
var durationSecs, threads, loops int
var objectData []byte
var objectDataMd5 string
var maxKeys, runningThreads, bucketCount, firstObject, objectCount, objectSize, opCounter int64
var objectCountFlag bool
var endtime time.Time
var interval float64
var version string = "development"

type IntervalStats struct {
	loop         int
	name         string
	mode         string
	bytes        int64
	slowdowns    int64
	intervalNano int64
	latNano      []int64
}

func (is *IntervalStats) makeOutputStats() OutputStats {
	// Compute and log the stats
	ops := len(is.latNano)
	totalLat := int64(0)
	minLat := float64(0)
	maxLat := float64(0)
	NinetyNineLat := float64(0)
	avgLat := float64(0)
	if ops > 0 {
		minLat = float64(is.latNano[0]) / 1000000
		maxLat = float64(is.latNano[ops-1]) / 1000000
		for i := range is.latNano {
			totalLat += is.latNano[i]
		}
		avgLat = float64(totalLat) / float64(ops) / 1000000
		NintyNineLatNano := is.latNano[int64(math.Round(0.99*float64(ops)))-1]
		NinetyNineLat = float64(NintyNineLatNano) / 1000000
	}
	seconds := float64(is.intervalNano) / 1000000000
	mbps := float64(is.bytes) / seconds / bytefmt.MEGABYTE
	iops := float64(ops) / seconds

	return OutputStats{
		is.loop,
		is.name,
		seconds,
		is.mode,
		ops,
		mbps,
		iops,
		minLat,
		avgLat,
		NinetyNineLat,
		maxLat,
		is.slowdowns}
}

type OutputStats struct {
	Loop          int
	IntervalName  string
	Seconds       float64
	Mode          string
	Ops           int
	Mbps          float64
	Iops          float64
	MinLat        float64
	AvgLat        float64
	NinetyNineLat float64
	MaxLat        float64
	Slowdowns     int64
}

func (o *OutputStats) log() {
	log.Printf(
		"Loop: %d, Int: %s, Dur(s): %.1f, Mode: %s, Ops: %d, MB/s: %.2f, IO/s: %.0f, Lat(ms): [ min: %.1f, avg: %.1f, 99%%: %.1f, max: %.1f ], Slowdowns: %d",
		o.Loop,
		o.IntervalName,
		o.Seconds,
		o.Mode,
		o.Ops,
		o.Mbps,
		o.Iops,
		o.MinLat,
		o.AvgLat,
		o.NinetyNineLat,
		o.MaxLat,
		o.Slowdowns)
}

func (o *OutputStats) csvHeader(w *csv.Writer) {
	if w == nil {
		log.Fatalln("OutputStats passed nil CSV writer")
	}

	s := []string{
		"Loop",
		"Inteval",
		"Duration(s)",
		"Mode", "Ops",
		"MB/s",
		"IO/s",
		"Min Latency (ms)",
		"Avg Latency(ms)",
		"99% Latency(ms)",
		"Max Latency(ms)",
		"Slowdowns"}

	if err := w.Write(s); err != nil {
		log.Fatalln("Error writing to CSV writer:", err)
	}
}

func (o *OutputStats) csv(w *csv.Writer) {
	if w == nil {
		log.Fatalln("OutputStats passed nil CSV writer")
	}

	s := []string{
		strconv.Itoa(o.Loop),
		o.IntervalName,
		strconv.FormatFloat(o.Seconds, 'f', 2, 64),
		o.Mode,
		strconv.Itoa(o.Ops),
		strconv.FormatFloat(o.Mbps, 'f', 2, 64),
		strconv.FormatFloat(o.Iops, 'f', 2, 64),
		strconv.FormatFloat(o.MinLat, 'f', 2, 64),
		strconv.FormatFloat(o.AvgLat, 'f', 2, 64),
		strconv.FormatFloat(o.NinetyNineLat, 'f', 2, 64),
		strconv.FormatFloat(o.MaxLat, 'f', 2, 64),
		strconv.FormatInt(o.Slowdowns, 10)}

	if err := w.Write(s); err != nil {
		log.Fatalln("Error writing to CSV writer:", err)
	}
}

func (o *OutputStats) json(jfile *os.File) {
	if jfile == nil {
		log.Fatalln("OutputStats passed nil JSON file")
	}
	jdata, err := json.Marshal(o)
	if err != nil {
		log.Fatalln("Error marshaling JSON:", err)
	}
	log.Println(string(jdata))
	_, err = jfile.WriteString(string(jdata) + "\n")
	if err != nil {
		log.Fatalln("Error writing to JSON file:", err)
	}
}

type ThreadStats struct {
	start       int64
	curInterval int64
	intervals   []IntervalStats
}

func makeThreadStats(s int64, loop int, mode string, intervalNano int64) ThreadStats {
	ts := ThreadStats{s, 0, []IntervalStats{}}
	ts.intervals = append(ts.intervals, IntervalStats{loop, "0", mode, 0, 0, intervalNano, []int64{}})
	return ts
}

func (ts *ThreadStats) updateIntervals(loop int, mode string, intervalNano int64) int64 {
	// Interval statistics disabled, so just return the current interval
	if intervalNano < 0 {
		return ts.curInterval
	}
	for ts.start+intervalNano*(ts.curInterval+1) < time.Now().UnixNano() {
		ts.curInterval++
		ts.intervals = append(
			ts.intervals,
			IntervalStats{
				loop,
				strconv.FormatInt(ts.curInterval, 10),
				mode,
				0,
				0,
				intervalNano,
				[]int64{}})
	}
	return ts.curInterval
}

func (ts *ThreadStats) finish() {
	ts.curInterval = -1
}

type Stats struct {
	// threads
	threads int
	// The loop we are in
	loop int
	// Test mode being run
	mode string
	// start time in nanoseconds
	startNano int64
	// end time in nanoseconds
	endNano int64
	// Duration in nanoseconds for each interval
	intervalNano int64
	// Per-thread statistics
	threadStats []ThreadStats
	// a map of per-interval thread completion counters
	intervalCompletions sync.Map
	// a counter of how many threads have finished updating stats entirely
	completions int32
}

func makeStats(loop int, mode string, threads int, intervalNano int64) Stats {
	start := time.Now().UnixNano()
	s := Stats{threads, loop, mode, start, 0, intervalNano, []ThreadStats{}, sync.Map{}, 0}
	for i := 0; i < threads; i++ {
		s.threadStats = append(s.threadStats, makeThreadStats(start, s.loop, s.mode, s.intervalNano))
		s.updateIntervals(i)
	}
	return s
}

func (stats *Stats) makeOutputStats(i int64) (OutputStats, bool) {
	// Check bounds first
	if stats.intervalNano < 0 || i < 0 {
		return OutputStats{}, false
	}
	// Not safe to log if not all writers have completed.
	value, ok := stats.intervalCompletions.Load(i)
	if !ok {
		return OutputStats{}, false
	}
	cp, ok := value.(*int32)
	if !ok {
		return OutputStats{}, false
	}
	count := atomic.LoadInt32(cp)
	if count < int32(stats.threads) {
		return OutputStats{}, false
	}

	bytes := int64(0)
	ops := int64(0)
	slowdowns := int64(0)

	for t := 0; t < stats.threads; t++ {
		bytes += stats.threadStats[t].intervals[i].bytes
		ops += int64(len(stats.threadStats[t].intervals[i].latNano))
		slowdowns += stats.threadStats[t].intervals[i].slowdowns
	}
	// Aggregate the per-thread Latency slice
	tmpLat := make([]int64, ops)
	var c int
	for t := 0; t < stats.threads; t++ {
		c += copy(tmpLat[c:], stats.threadStats[t].intervals[i].latNano)
	}
	sort.Slice(tmpLat, func(i, j int) bool { return tmpLat[i] < tmpLat[j] })
	is := IntervalStats{stats.loop, strconv.FormatInt(i, 10), stats.mode, bytes, slowdowns, stats.intervalNano, tmpLat}
	return is.makeOutputStats(), true
}

func (stats *Stats) makeTotalStats() (OutputStats, bool) {
	// Not safe to log if not all writers have completed.
	completions := atomic.LoadInt32(&stats.completions)
	if completions < int32(threads) {
		log.Printf("log, completions: %d", completions)
		return OutputStats{}, false
	}

	bytes := int64(0)
	ops := int64(0)
	slowdowns := int64(0)

	for t := 0; t < stats.threads; t++ {
		for i := 0; i < len(stats.threadStats[t].intervals); i++ {
			bytes += stats.threadStats[t].intervals[i].bytes
			ops += int64(len(stats.threadStats[t].intervals[i].latNano))
			slowdowns += stats.threadStats[t].intervals[i].slowdowns
		}
	}
	// Aggregate the per-thread Latency slice
	tmpLat := make([]int64, ops)
	var c int
	for t := 0; t < stats.threads; t++ {
		for i := 0; i < len(stats.threadStats[t].intervals); i++ {
			c += copy(tmpLat[c:], stats.threadStats[t].intervals[i].latNano)
		}
	}
	sort.Slice(tmpLat, func(i, j int) bool { return tmpLat[i] < tmpLat[j] })
	is := IntervalStats{stats.loop, "TOTAL", stats.mode, bytes, slowdowns, stats.endNano - stats.startNano, tmpLat}
	return is.makeOutputStats(), true
}

// Only safe to call from the calling thread
func (stats *Stats) updateIntervals(threadNum int) int64 {
	curInterval := stats.threadStats[threadNum].curInterval
	newInterval := stats.threadStats[threadNum].updateIntervals(stats.loop, stats.mode, stats.intervalNano)

	// Finish has already been called
	if curInterval < 0 {
		return -1
	}

	for i := curInterval; i < newInterval; i++ {
		// load or store the current value
		value, _ := stats.intervalCompletions.LoadOrStore(i, new(int32))
		cp, ok := value.(*int32)
		if !ok {
			log.Printf("updateIntervals: got data of type %T but wanted *int32", value)
			continue
		}

		count := atomic.AddInt32(cp, 1)
		if count == int32(stats.threads) {
			if is, ok := stats.makeOutputStats(i); ok {
				is.log()
			}
		}
	}
	return newInterval
}

func (stats *Stats) addOp(threadNum int, bytes int64, latNano int64) {

	// Interval statistics
	cur := stats.threadStats[threadNum].curInterval
	if cur < 0 {
		return
	}
	stats.threadStats[threadNum].intervals[cur].bytes += bytes
	stats.threadStats[threadNum].intervals[cur].latNano =
		append(stats.threadStats[threadNum].intervals[cur].latNano, latNano)
}

func (stats *Stats) addSlowDown(threadNum int) {
	cur := stats.threadStats[threadNum].curInterval
	stats.threadStats[threadNum].intervals[cur].slowdowns++
}

func (stats *Stats) finish(threadNum int) {
	stats.updateIntervals(threadNum)
	stats.threadStats[threadNum].finish()
	count := atomic.AddInt32(&stats.completions, 1)
	if count == int32(stats.threads) {
		stats.endNano = time.Now().UnixNano()
	}
}

func runUpload(threadNum int, fendtime time.Time, stats *Stats) {
	errcnt := 0
	svc := s3.New(session.New(), cfg)
	for {
		if durationSecs > -1 && time.Now().After(endtime) {
			break
		}
		objnum := atomic.AddInt64(&opCounter, 1)
		bucketNum := objnum % int64(bucketCount)
		if objectCount > -1 && objnum >= objectCount {
			objnum = atomic.AddInt64(&opCounter, -1)
			break
		}
		fileobj := bytes.NewReader(objectData)

		key := fmt.Sprintf("%s%012d", objectPrefix, objnum)
		r := &s3.PutObjectInput{
			Bucket: &buckets[bucketNum],
			Key:    &key,
			Body:   fileobj,
		}
		start := time.Now().UnixNano()
		req, _ := svc.PutObjectRequest(r)
		// Disable payload checksum calculation (very expensive)
		req.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		err := req.Send()
		end := time.Now().UnixNano()
		stats.updateIntervals(threadNum)

		if err != nil {
			errcnt++
			stats.addSlowDown(threadNum)
			atomic.AddInt64(&opCounter, -1)
			log.Print("upload error: ", err)
		} else {
			// Update the stats
			stats.addOp(threadNum, objectSize, end-start)
		}
		if errcnt > 2 {
			break
		}
	}
	stats.finish(threadNum)
	atomic.AddInt64(&runningThreads, -1)
}

func readBody(r io.Reader) (int64, error) {
	var bytesRead int64
	buf := make([]byte, 65536)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			bytesRead += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				return bytesRead, nil
			}
			return bytesRead, err
		}
	}
}

func runDownload(threadNum int, fendtime time.Time, stats *Stats) {
	errcnt := 0
	svc := s3.New(session.New(), cfg)
	for {
		if durationSecs > -1 && time.Now().After(endtime) {
			break
		}

		var objnum int64
		if objectCount > -1 {
			// Run random download if the number of objects is known
			objnum = rand.Int63() % objectCount
		} else {
			objnum = atomic.AddInt64(&opCounter, 1)
			if objectCount > -1 && objnum >= objectCount {
				atomic.AddInt64(&opCounter, -1)
				break
			}
		}

		bucketNum := objnum % int64(bucketCount)
		key := fmt.Sprintf("%s%012d", objectPrefix, objnum)
		r := &s3.GetObjectInput{
			Bucket: &buckets[bucketNum],
			Key:    &key,
		}

		start := time.Now().UnixNano()
		req, resp := svc.GetObjectRequest(r)
		err := req.Send()
		end := time.Now().UnixNano()
		stats.updateIntervals(threadNum)
		if err != nil {
			errcnt++
			stats.addSlowDown(threadNum)
			log.Print("download error: ", err)
		} else {
			var bytesRead int64
			defer resp.Body.Close()
			bytesRead, err := readBody(resp.Body)
			// Update the stats
			stats.addOp(threadNum, bytesRead, end-start)
			if err != nil {
				errcnt++
				stats.addSlowDown(threadNum)
				log.Print("download error: ", err)
			}
		}
		if errcnt > 2 {
			break
		}

	}
	stats.finish(threadNum)
	atomic.AddInt64(&runningThreads, -1)
}

func runDelete(threadNum int, stats *Stats) {
	errcnt := 0
	svc := s3.New(session.New(), cfg)
	for {
		if durationSecs > -1 && time.Now().After(endtime) {
			break
		}

		objnum := atomic.AddInt64(&opCounter, 1)
		if objectCount > -1 && objnum >= objectCount {
			atomic.AddInt64(&opCounter, -1)
			break
		}

		bucketNum := objnum % int64(bucketCount)

		key := fmt.Sprintf("%s%012d", objectPrefix, objnum)
		r := &s3.DeleteObjectInput{
			Bucket: &buckets[bucketNum],
			Key:    &key,
		}

		start := time.Now().UnixNano()
		req, out := svc.DeleteObjectRequest(r)
		err := req.Send()
		end := time.Now().UnixNano()
		stats.updateIntervals(threadNum)

		if err != nil {
			errcnt++
			stats.addSlowDown(threadNum)
			log.Print("delete error: ", err, " out ", out.String())
		} else {
			// Update the stats
			stats.addOp(threadNum, objectSize, end-start)
		}
		if errcnt > 2 {
			break
		}
	}
	stats.finish(threadNum)
	atomic.AddInt64(&runningThreads, -1)
}

func runBucketDelete(threadNum int, stats *Stats) {
	svc := s3.New(session.New(), cfg)

	for {
		bucketNum := atomic.AddInt64(&opCounter, 1)
		if bucketNum >= bucketCount {
			atomic.AddInt64(&opCounter, -1)
			break
		}
		r := &s3.DeleteBucketInput{
			Bucket: &buckets[bucketNum],
		}

		start := time.Now().UnixNano()
		_, err := svc.DeleteBucket(r)
		end := time.Now().UnixNano()
		stats.updateIntervals(threadNum)

		if err != nil {
			break
		}
		stats.addOp(threadNum, 0, end-start)
	}
	stats.finish(threadNum)
	atomic.AddInt64(&runningThreads, -1)
}

func runBucketList(threadNum int, stats *Stats) {
	svc := s3.New(session.New(), cfg)

	marker := ""
	bucketNum := rand.Int63() % bucketCount
	for {
		if durationSecs > -1 && time.Now().After(endtime) {
			break
		}

		start := time.Now().UnixNano()
		p, err := svc.ListObjects(&s3.ListObjectsInput{
			Bucket:  &buckets[bucketNum],
			Marker:  &marker,
			MaxKeys: &maxKeys,
		})
		end := time.Now().UnixNano()

		if err != nil {
			break
		}
		stats.addOp(threadNum, 0, end-start)
		stats.updateIntervals(threadNum)

		if *p.IsTruncated {
			marker = *p.NextMarker
		} else {
			marker = ""
			bucketNum = rand.Int63() % bucketCount
		}
	}
	stats.finish(threadNum)
	atomic.AddInt64(&runningThreads, -1)
}

var cfg *aws.Config

func runBucketsInit(threadNum int, stats *Stats) {
	svc := s3.New(session.New(), cfg)

	for {
		bucketNum := atomic.AddInt64(&opCounter, 1)
		if bucketNum >= bucketCount {
			atomic.AddInt64(&opCounter, -1)
			break
		}
		start := time.Now().UnixNano()
		in := &s3.CreateBucketInput{Bucket: aws.String(buckets[bucketNum])}
		_, err := svc.CreateBucket(in)
		end := time.Now().UnixNano()
		stats.updateIntervals(threadNum)

		if err != nil {
			if !strings.Contains(err.Error(), s3.ErrCodeBucketAlreadyOwnedByYou) &&
				!strings.Contains(err.Error(), "BucketAlreadyExists") {
				log.Fatalf("FATAL: Unable to create bucket %s (is your access and secret correct?): %v\n", buckets[bucketNum], err)
			}
		}
		stats.addOp(threadNum, 0, end-start)
	}
	stats.finish(threadNum)
	atomic.AddInt64(&runningThreads, -1)
}

type pagedObject struct {
	bucketNum int64
	key       string
	size      int64
}

func runPagedList(wg *sync.WaitGroup, bucketNum int64, list chan<- pagedObject) {
	svc := s3.New(session.New(), cfg)
	err := svc.ListObjectsPages(
		&s3.ListObjectsInput{
			Bucket:  &buckets[bucketNum],
			MaxKeys: &maxKeys,
		},
		func(page *s3.ListObjectsOutput, last bool) bool {
			for _, v := range page.Contents {
				list <- pagedObject{
					bucketNum: bucketNum,
					key:       *v.Key,
					size:      *v.Size,
				}
			}
			return true
		})
	if err != nil {
		log.Print("s3.ListObjectsPages() error: ", err)
	}
	wg.Done()
}

func runBucketsClear(list <-chan pagedObject, threadNum int, stats *Stats) {
	svc := s3.New(session.New(), cfg)

	for {
		v := <-list
		start := time.Now().UnixNano()
		_, err := svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: &buckets[v.bucketNum],
			Key:    &v.key,
		})
		end := time.Now().UnixNano()
		stats.updateIntervals(threadNum)
		if err != nil {
			break
		}
		stats.addOp(threadNum, v.size, end-start)
	}
	stats.finish(threadNum)
	atomic.AddInt64(&runningThreads, -1)
}

func runWrapper(loop int, r rune) []OutputStats {
	opCounter = firstObject - 1
	runningThreads = int64(threads)
	intervalNano := int64(interval * 1000000000)
	endtime = time.Now().Add(time.Second * time.Duration(durationSecs))
	var stats Stats

	// If we perviously set the object count after running a put
	// test, set the object count back to -1 for the new put test.
	if r == 'p' && objectCountFlag {
		objectCount = -1
		objectCountFlag = false
	}

	switch r {
	case 'c':
		log.Printf("Running Loop %d BUCKET CLEAR TEST", loop)
		stats = makeStats(loop, "BCLR", threads, intervalNano)
		list := make(chan pagedObject, threads*2)
		var wg = sync.WaitGroup{}
		for b := int64(0); b < bucketCount; b++ {
			wg.Add(1)
			go runPagedList(&wg, b, list)
		}
		for n := 0; n < threads; n++ {
			go runBucketsClear(list, n, &stats)
		}
		wg.Wait()
		close(list)
	case 'x':
		log.Printf("Running Loop %d BUCKET DELETE TEST", loop)
		stats = makeStats(loop, "BDEL", threads, intervalNano)
		for n := 0; n < threads; n++ {
			go runBucketDelete(n, &stats)
		}
	case 'i':
		log.Printf("Running Loop %d BUCKET INIT TEST", loop)
		stats = makeStats(loop, "BINIT", threads, intervalNano)
		for n := 0; n < threads; n++ {
			go runBucketsInit(n, &stats)
		}
	case 'p':
		log.Printf("Running Loop %d OBJECT PUT TEST", loop)
		stats = makeStats(loop, "PUT", threads, intervalNano)
		for n := 0; n < threads; n++ {
			go runUpload(n, endtime, &stats)
		}
	case 'l':
		log.Printf("Running Loop %d BUCKET LIST TEST", loop)
		stats = makeStats(loop, "LIST", threads, intervalNano)
		for n := 0; n < threads; n++ {
			go runBucketList(n, &stats)
		}
	case 'g':
		log.Printf("Running Loop %d OBJECT GET TEST", loop)
		stats = makeStats(loop, "GET", threads, intervalNano)
		for n := 0; n < threads; n++ {
			go runDownload(n, endtime, &stats)
		}
	case 'd':
		log.Printf("Running Loop %d OBJECT DELETE TEST", loop)
		stats = makeStats(loop, "DEL", threads, intervalNano)
		for n := 0; n < threads; n++ {
			go runDelete(n, &stats)
		}
	}

	// Wait for it to finish
	for atomic.LoadInt64(&runningThreads) > 0 {
		time.Sleep(time.Millisecond)
	}

	// If the user didn't set the objectCount, we can set it here
	// to limit subsequent get/del tests to valid objects only.
	if r == 'p' && objectCount < 0 {
		objectCount = opCounter + 1
		objectCountFlag = true
	}

	// Create the Output Stats
	os := make([]OutputStats, 0)
	for i := int64(0); i >= 0; i++ {
		if o, ok := stats.makeOutputStats(i); ok {
			os = append(os, o)
		} else {
			break
		}
	}
	if o, ok := stats.makeTotalStats(); ok {
		o.log()
		os = append(os, o)
	}
	return os
}

func initParameters() {
	// Parse command line
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.StringVar(&accessKey, "a", os.Getenv("AWS_ACCESS_KEY_ID"), "Access key")
	myflag.StringVar(&secretSey, "s", os.Getenv("AWS_SECRET_ACCESS_KEY"), "Secret key")
	myflag.StringVar(&urlHost, "u", os.Getenv("AWS_HOST"), "URL for host with method prefix")
	myflag.StringVar(&objectPrefix, "op", "", "Prefix for objects")
	myflag.StringVar(&bucketPrefix, "bp", "hotsauce-bench", "Prefix for buckets")
	myflag.StringVar(&bucketList, "bl", "", "Use space-separated list of buckets for testing, not <prefix>000000000000")
	myflag.StringVar(&region, "r", "us-east-1", "Region for testing")
	myflag.StringVar(&modes, "m", modeString, "Run modes in order.  See NOTES for more info")
	myflag.StringVar(&output, "o", "", "Write CSV output to this file")
	myflag.StringVar(&jsonOutput, "j", "", "Write JSON output to this file")
	myflag.Int64Var(&maxKeys, "mk", 1000, "Maximum number of keys to retreive at once for bucket listings")
	myflag.Int64Var(&objectCount, "n", -1, "Maximum number of objects <-1 for unlimited>")
	myflag.Int64Var(&firstObject, "f", 0, "Object number to start with")
	myflag.Int64Var(&bucketCount, "b", 1, "Number of buckets to distribute IOs across")
	myflag.IntVar(&durationSecs, "d", 60, "Maximum test duration in seconds <-1 for unlimited>")
	myflag.IntVar(&threads, "t", 1, "Number of threads to run")
	myflag.IntVar(&loops, "l", 1, "Number of times to repeat test")
	myflag.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with postfix K, M, and G")
	myflag.Float64Var(&interval, "ri", 1.0, "Number of seconds between report intervals")
	// define custom usage output with notes
	notes := `
NOTES:
  - Valid mode types for the -m mode string are:
    c: clear all existing objects from buckets (requires lookups)
    x: delete buckets
    i: initialize buckets
    p: put objects in buckets
    l: list objects in buckets
    g: get objects from buckets (randomly when object count is known, sequentially otherwise)
    d: delete objects from buckets

    These modes are processed in-order and can be repeated, ie "ippgd" will
    initialize the buckets, put the objects, reput the objects, get the
    objects, and then delete the objects.  The repeat flag will repeat this
    whole process the specified number of times.

  - When performing bucket listings, many S3 storage systems limit the
    maximum number of keys returned to 1000 even if MaxKeys is set higher.
    hsbench will attempt to set MaxKeys to whatever value is passed via the
    "mk" flag, but it's likely that any values above 1000 will be ignored.
`
	myflag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "\nUSAGE:", os.Args[0], "[OPTIONS]")
		fmt.Fprintln(flag.CommandLine.Output(), "\nOPTIONS:")
		myflag.PrintDefaults()
		fmt.Fprint(flag.CommandLine.Output(), notes)
	}

	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	// Check the arguments
	if objectCount < 0 && durationSecs < 0 {
		log.Fatalln("The number of objects and duration can not both be unlimited")
	}
	if urlHost == "" {
		log.Fatalln("Missing argument -u for host endpoint")
	}
	invalidMode := false
	for _, r := range modes {
		if !strings.ContainsRune(modeString, r) {
			log.Printf("Invalid mode '%c' passed to -m", r)
			invalidMode = true
		}
	}
	if invalidMode {
		log.Fatalln("Invalid modes passed to -m, see help for details")
	}
	var err error
	var size uint64
	if size, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalln("Invalid -z argument for object size:", err)
	}
	objectSize = int64(size)
}

func initData() {
	// Initialize data for the bucket
	objectData = make([]byte, objectSize)
	rand.Read(objectData)
	hasher := md5.New()
	hasher.Write(objectData)
	objectDataMd5 = base64.StdEncoding.EncodeToString(hasher.Sum(nil))
}

func main() {
	// Hello
	log.Print("Hotsauce S3 Benchmark Version ", version)
	initParameters()

	var creds *credentials.Credentials
	if accessKey != "" && secretSey != "" {
		creds = credentials.NewStaticCredentials(accessKey, secretSey, "")
	}

	cfg = &aws.Config{
		Endpoint:    aws.String(urlHost),
		Credentials: creds,
		Region:      aws.String(region),
		// DisableParamValidation:  aws.Bool(true),
		DisableComputeChecksums: aws.Bool(true),
		S3ForcePathStyle:        aws.Bool(true),
	}

	// Echo the parameters
	log.Print("Parameters:")
	log.Printf("url=%s", urlHost)
	log.Printf("object_prefix=%s", objectPrefix)
	if bucketList != "" {
		log.Printf("bucket_list=%s", bucketList)
	} else {
		log.Printf("bucket_prefix=%s", bucketPrefix)
	}
	log.Printf("region=%s", region)
	log.Printf("modes=%s", modes)
	log.Printf("output=%s", output)
	log.Printf("json_output=%s", jsonOutput)
	log.Printf("max_keys=%d", maxKeys)
	log.Printf("object_count=%d", objectCount)
	log.Printf("first_object=%d", firstObject)
	log.Printf("bucket_count=%d", bucketCount)
	log.Printf("duration=%d", durationSecs)
	log.Printf("threads=%d", threads)
	log.Printf("loops=%d", loops)
	log.Printf("size=%s", sizeArg)
	log.Printf("interval=%f", interval)

	// Init Data
	initData()

	// Setup the slice of buckets
	if bucketList == "" {
		for i := int64(0); i < bucketCount; i++ {
			buckets = append(buckets, fmt.Sprintf("%s%012d", bucketPrefix, i))
		}
	} else {
		buckets = strings.Split(bucketList, " ")
	}

	// Loop running the tests
	oStats := make([]OutputStats, 0)
	for loop := 0; loop < loops; loop++ {
		for _, r := range modes {
			oStats = append(oStats, runWrapper(loop, r)...)
		}
	}

	// Write CSV Output
	if output != "" {
		file, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			log.Fatalln("Could not open CSV file for writing")
		}
		defer file.Close()

		csvWriter := csv.NewWriter(file)
		for i, o := range oStats {
			if i == 0 {
				o.csvHeader(csvWriter)
			}
			o.csv(csvWriter)
		}
		csvWriter.Flush()
	}

	// Write JSON output
	if jsonOutput != "" {
		file, err := os.OpenFile(jsonOutput, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			log.Fatalln("Could not open JSON file for writing")
		}
		defer file.Close()
		data, err := json.Marshal(oStats)
		if err != nil {
			log.Fatalln("Error marshaling JSON:", err)
		}
		_, err = file.Write(data)
		if err != nil {
			log.Fatalln("Error writing to JSON file:", err)
		}

		err = file.Sync()
		if err != nil {
			log.Print("file.Sync() error: ", err)

		}
	}
}
