package data

import (
	"errors"
	"bufio"
	"log"
	"reflect"
	"sync"
	"net"
	"time"

	riakprotobuf "github.com/likemindnetworks/riakdataportation/protobuf"
	riakcli "github.com/likemindnetworks/riakdataportation/client"
)

var (
	AlreadyRunning = errors.New("Export is already running")
)

type Export struct {
	cli *riakcli.Client
	filterFn func([]byte) bool
	bucketTypes []string
	dtBucketTypes []string
	output *bufio.Writer
	outputChan chan []byte
	errorChan chan error
	wg sync.WaitGroup
}

func NewExport(
		cli *riakcli.Client,
		filterFn func([]byte) bool,
		bucketTypes []string,
		dtBucketTypes []string,
		output *bufio.Writer,
) *Export {
	// filter buckets
	if (filterFn == nil) {
		filterFn = func([]byte) bool { return true }
	}

	return &Export{
		cli: cli,
		filterFn: filterFn,
		bucketTypes: bucketTypes,
		dtBucketTypes: dtBucketTypes,
		output: output,
	}
}

func (e *Export) Run() (err error) {

	if e.outputChan != nil {
		return AlreadyRunning
	}

	startTime := time.Now()
	log.Printf(
		"Exporting the following bucket types %s %s",
		e.bucketTypes, e.dtBucketTypes,
	)

	e.outputChan = make(chan []byte)
	e.errorChan = make(chan error)

	totalByteCnt := 0
	quit := make(chan int)

	go func() {
		for {
			select {
			case data, ok := <- e.outputChan:
				if ok {
					totalByteCnt += len(data)
					_, err = e.output.Write(data)
				} else {
					err = e.output.Flush()
					goto QUIT
				}
			case err = <- e.errorChan:
				goto QUIT
			}
		}

QUIT:
		quit <- 0
	}()

	e.wg.Add(len(e.bucketTypes))
	for _, bt := range e.bucketTypes {
		go e.processBucketType([]byte(bt))
	}

	e.wg.Add(len(e.dtBucketTypes))
	for _, bt := range e.dtBucketTypes {
		go e.processBucketType([]byte(bt))
	}

	e.wg.Wait()

	// no more data after this point
	close(e.outputChan)
	close(e.errorChan)

	<-quit

	if err == nil {
		log.Printf(
			"%d total bytes writen in %s", totalByteCnt, time.Since(startTime),
		)
	}

	e.outputChan = nil
	e.errorChan  = nil
	return
}

func (e *Export) isRiakDtBucket(bucket []byte) bool {
	for _, b := range e.dtBucketTypes {
		if reflect.DeepEqual(bucket, b) {
			return true
		}
	}

	return false
}

func (e *Export) processBucketType(bt []byte) {
	defer e.wg.Done()

	isStreaming := true
	req := riakprotobuf.RpbListBucketsReq{ Type: bt, Stream: &isStreaming }

	err, conn, _ := e.cli.SendMessage(&req, riakprotobuf.CodeRpbListBucketsReq)

	if err != nil {
		e.errorChan <- err
		return
	} else {
		defer e.cli.ReleaseConnection(conn)
	}

	var (
		partial *riakprotobuf.RpbListBucketsResp
		buckets [][]byte
		filteredBuckets [][]byte
	)

	for {
		partial = &riakprotobuf.RpbListBucketsResp{}
		err = e.cli.ReceiveMessage(conn, partial, true)
		if (err != nil) {
			e.errorChan <- err
			return
		}

		buckets = append(buckets, partial.Buckets...)

		if partial.Done != nil {
			break;
		}
	}

	for _, b := range buckets {
		if (e.filterFn(b)) {
			filteredBuckets = append(filteredBuckets, b)
		}
	}

	filteredBucketsCnt := len(filteredBuckets)

	if filteredBucketsCnt == 0 {
		return
	}

	// fan out process
	log.Printf(
		"%d buckets to be to processed of bucket type [%s] (out of %d)",
		filteredBucketsCnt, bt, len(buckets),
	)

	e.wg.Add(filteredBucketsCnt)

	for _, b := range filteredBuckets {
		go e.processBucket(bt, b)
	}
}

func (e *Export) processBucket(bt []byte, bucket []byte) {
	defer e.wg.Done()

	req := riakprotobuf.RpbListKeysReq{ Type: bt, Bucket: bucket }

	err, conn, _ := e.cli.SendMessage(&req, riakprotobuf.CodeRpbListKeysReq)
	if err != nil {
		e.errorChan <- err
		return
	} else {
		defer e.cli.ReleaseConnection(conn)
	}

	var (
		partial *riakprotobuf.RpbListKeysResp
		keys [][]byte
	)

	for {
		partial = &riakprotobuf.RpbListKeysResp{}
		err = e.cli.ReceiveMessage(conn, partial, true)
		if (err != nil) {
			e.errorChan <- err
			return
		}

		keys = append(keys, partial.Keys...)

		if partial.Done != nil {
			break;
		}
	}

	// fan out process
	keyCnt := len(keys)

	log.Printf(
		"%d keys to be to processed in bucket [%s] of bucket type [%s]",
		keyCnt, bucket, bt,
	)

	e.wg.Add(keyCnt)

	for _, key := range keys {
		go e.processKey(bt, bucket, key)
	}
}

func (e *Export) processKey(bt []byte, bucket []byte, key []byte) {
	defer e.wg.Done()

	var (
		err error
		conn *net.TCPConn
		reqbuf []byte
	)

	if (e.isRiakDtBucket(bucket)) {
		req := riakprotobuf.DtFetchReq{
			Type: bt,
			Bucket: bucket,
			Key: key,
		}

		err, conn, reqbuf = e.cli.SendMessage(&req, riakprotobuf.CodeDtFetchReq)
	} else {
		req := riakprotobuf.RpbGetReq{
			Type: bt,
			Bucket: bucket,
			Key: key,
		}

		err, conn, reqbuf = e.cli.SendMessage(&req, riakprotobuf.CodeRpbGetReq)
	}

	if err != nil {
		e.errorChan <- err
		return
	}

	// no need to unmarshal this
	err, headerbuf, responsebuf := e.cli.ReceiveRawMessage(conn, false)
	if (err != nil) {
		e.errorChan <- err
		return
	}

	// output the raw messages
	// all messages should be self-decoding

	e.outputChan <- reqbuf
	e.outputChan <- headerbuf
	e.outputChan <- responsebuf
}
