package data

import (
	"errors"
	"bufio"
	"sync"
	"net"

	"log"

	riakprotobuf "github.com/likemindnetworks/riakdataportation/protobuf"
	riakcli "github.com/likemindnetworks/riakdataportation/client"
)

var (
	ExportAlreadyRunning = errors.New("Export is already running")
)

type Export struct {
	cli *riakcli.Client
	filterFn func([]byte) bool
	bucketTypes []string
	dtBucketTypes []string
	output *bufio.Writer
	outputChan chan []byte
	outputMutex sync.Mutex
	errorChan chan error
	unitOfWorkChan chan int
	unitOfWorkFinishedChan chan int
	wg sync.WaitGroup
	isDeleteAfter bool
}

func NewExport(
		cli *riakcli.Client,
		filterFn func([]byte) bool,
		bucketTypes []string,
		dtBucketTypes []string,
		output *bufio.Writer,
		isDeleteAfter bool,
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
		isDeleteAfter: isDeleteAfter,
	}
}

func (e *Export) Run(progressChan chan float64) (byteCnt int, err error) {

	if e.outputChan != nil {
		return 0, ExportAlreadyRunning
	}

	e.outputChan = make(chan []byte)
	e.errorChan = make(chan error)
	e.unitOfWorkChan = make(chan int, 100)
	e.unitOfWorkFinishedChan = make(chan int, 100)

	unitOfWorkFinished := 0
	unitOfWorkTotal := 0
	quit := make(chan int)

	go func() {
		for {
			select {
			case data, ok := <- e.outputChan:
				// collects all output bytes
				if ok {
					byteCnt += len(data)
					_, err = e.output.Write(data)
				} else {
					err = e.output.Flush()
					goto QUIT
				}
			case u := <- e.unitOfWorkFinishedChan:
				unitOfWorkFinished += u

				// update progress
				if progressChan != nil {
					if unitOfWorkTotal > 0 {
						progressChan <- float64(unitOfWorkFinished) /
							float64(unitOfWorkTotal)
					} else {
						progressChan <- 0
					}
				}
			case u := <- e.unitOfWorkChan:
				unitOfWorkTotal += u
			case err = <- e.errorChan:
				if err != nil {
					log.Printf("Error during import: %s", err);
				}

				goto QUIT
			}
		}

QUIT:
		quit <- 0
	}()

	bucketTypesCnt := len(e.bucketTypes)
	e.wg.Add(bucketTypesCnt)
	e.unitOfWorkChan <- bucketTypesCnt
	for _, bt := range e.bucketTypes {
		go e.processBucketType([]byte(bt))
	}

	dtBucketTypesCnt := len(e.dtBucketTypes)
	e.wg.Add(dtBucketTypesCnt)
	e.unitOfWorkChan <- dtBucketTypesCnt
	for _, bt := range e.dtBucketTypes {
		go e.processBucketType([]byte(bt))
	}

	// wait for all goroutinues
	e.wg.Wait()

	// no more data after this point
	close(e.outputChan)
	close(e.errorChan)

	// wait for io
	<-quit

	// reset channels
	e.outputChan = nil
	e.errorChan  = nil
	e.unitOfWorkChan = nil
	e.unitOfWorkFinishedChan  = nil

	// close progress
	if progressChan != nil {
		close(progressChan)
	}
	return
}

func (e *Export) isRiakDtBucketType(bt []byte) bool {
	for _, b := range e.dtBucketTypes {
		if string(bt) == b {
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
	e.wg.Add(filteredBucketsCnt)

	for _, b := range filteredBuckets {
		e.unitOfWorkChan <- 1
		go e.processBucket(bt, b)
	}

	e.unitOfWorkFinishedChan <- 1
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

	e.wg.Add(keyCnt)

	for _, key := range keys {
		go e.processKey(bt, bucket, key)
	}

	// update progress
	e.unitOfWorkFinishedChan <- 1
	e.unitOfWorkChan <- keyCnt
}

func (e *Export) processKey(bt []byte, bucket []byte, key []byte) {
	defer e.wg.Done()

	var (
		err error
		conn *net.TCPConn
		reqbuf []byte
	)

	if e.isRiakDtBucketType(bt) {
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

	// delete after output

	if e.isDeleteAfter {
		err := e.cli.ClearKey(bt, bucket, key, e.isRiakDtBucketType(bt))

		if err != nil {
			e.errorChan <- err
			return
		}
	}

	// output the raw messages
	// all messages should be self-decoding

	e.outputMutex.Lock()
	defer e.outputMutex.Unlock()

	e.outputChan <- reqbuf
	e.outputChan <- headerbuf
	e.outputChan <- responsebuf
	e.unitOfWorkFinishedChan <- 1
}
