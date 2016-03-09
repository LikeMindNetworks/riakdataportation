package data

import (
	"bufio"
	"fmt"
	"log"

	riakprotobuf "github.com/likemindnetworks/riakdataportation/protobuf"
	riakcli "github.com/likemindnetworks/riakdataportation/client"
)

type bucketProcessResult struct {
	bucketType []byte
	bucket []byte
	keyCnt int
}

func (b *bucketProcessResult) String() string {
	return fmt.Sprintf("%s~%s: %d keys", b.bucketType, b.bucket, b.keyCnt)
}

func Export(
		cli *riakcli.Client,
		filterFn func([]byte) bool,
		bucketTypes []string,
		dtBucketTypes []string,
		output *bufio.Writer,
) (err error) {
	for _, bt := range bucketTypes {
		err = processBucketType(cli, filterFn, []byte(bt), false, output)

		if (err != nil) {
			break
		}
	}

	for _, bt := range dtBucketTypes {
		err = processBucketType(cli, filterFn, []byte(bt), true, output)

		if (err != nil) {
			break
		}
	}

	return
}

func processBucketType(
		cli *riakcli.Client,
		filterFn func([]byte) bool,
		bt []byte,
		isRiakDt bool,
		output *bufio.Writer,
) (err error) {
	isStreaming := true
	req := riakprotobuf.RpbListBucketsReq{ Type: bt, Stream: &isStreaming }

	err, conn, _ := cli.SendMessage(&req, riakprotobuf.CodeRpbListBucketsReq)
	if err != nil {
		return
	}

	var (
		partial *riakprotobuf.RpbListBucketsResp
		buckets [][]byte
		filteredBuckets [][]byte
	)

	for {
		partial = &riakprotobuf.RpbListBucketsResp{}
		err = cli.ReceiveMessage(conn, partial, true)
		if (err != nil) {
			return
		}

		buckets = append(buckets, partial.Buckets...)

		if partial.Done != nil {
			break;
		}
	}

	cli.ReleaseConnection(conn)

	// filter buckets
	if (filterFn == nil) {
		filterFn = func([]byte) bool { return true }
	}

	for _, b := range buckets {
		if (filterFn(b)) {
			filteredBuckets = append(filteredBuckets, b)
		}
	}

	filteredBucketsCnt := len(filteredBuckets)

	log.Printf(
		"%d buckets retrieved in type [%s] %d to be processed",
		len(buckets),
		bt,
		filteredBucketsCnt,
	)

	if filteredBucketsCnt == 0 {
		return
	}

	// process
	resultChan := make(chan bucketProcessResult)
	errorChan := make(chan error)

	for _, b := range filteredBuckets {
		go processBucket(cli, bt, b, isRiakDt, resultChan, errorChan, output)
	}

	// and wait
	remain := filteredBucketsCnt

	for {
		select {
		case r := <-resultChan:
			log.Printf("processed %s", &r)
			remain -= 1

			if remain == 0 {
				return
			}
		case err = <-errorChan:
			// abort
			return
		}
	}
}

func processBucket(
		cli *riakcli.Client,
		bt []byte, bucket []byte,
		isRiakDt bool,
		resultChan chan bucketProcessResult,
		errorChan chan error,
		output *bufio.Writer,
) {
	req := riakprotobuf.RpbListKeysReq{ Type: bt, Bucket: bucket }

	err, conn, _ := cli.SendMessage(&req, riakprotobuf.CodeRpbListKeysReq)
	if err != nil {
		errorChan <- err
		return
	}

	var (
		partial *riakprotobuf.RpbListKeysResp
		keys [][]byte
	)

	for {
		partial = &riakprotobuf.RpbListKeysResp{}
		err = cli.ReceiveMessage(conn, partial, true)
		if (err != nil) {
			errorChan <- err
			return
		}

		keys = append(keys, partial.Keys...)

		if partial.Done != nil {
			break;
		}
	}

	cli.ReleaseConnection(conn)

	// process
	outputChan := make(chan []byte)
	childErrorChan := make(chan error)
	keyCnt := len(keys)

	for _, key := range keys {
		if isRiakDt {
			go processDT(cli, bt, bucket, key, outputChan, childErrorChan)
		} else {
			go processKV(cli, bt, bucket, key, outputChan, childErrorChan)
		}
	}

	// and wait
	remain := keyCnt

	for {
		select {
		case data := <-outputChan:
			if (data == nil) {
				// end of input
				if remain -= 1; remain == 0 {
					// done
					resultChan <- bucketProcessResult{
						bucketType: bt,
						bucket: bucket,
						keyCnt: keyCnt,
					};

					return
				}
			} else {
				_, err = output.Write(data)
			}
		case err = <-errorChan:
			return
		}

		if err != nil {
			// pass it up, then abort
			errorChan <- err
			return
		}
	}
}

func processKV(
		cli *riakcli.Client,
		bt []byte, bucket []byte, key []byte,
		outputChan chan []byte,
		errorChan chan error,
) {
	req := riakprotobuf.RpbGetReq{
		Type: bt,
		Bucket: bucket,
		Key: key,
	}

	err, conn, reqbuf := cli.SendMessage(&req, riakprotobuf.CodeRpbGetReq)
	if err != nil {
		errorChan <- err
		return
	}

	// no need to unmarshal this
	err, headerbuf, responsebuf := cli.ReceiveRawMessage(conn, false)
	if (err != nil) {
		errorChan <- err
		return
	}

	// output the raw messages
	// all messages should be self-decoding

	outputChan <- reqbuf
	outputChan <- headerbuf
	outputChan <- responsebuf
	outputChan <- nil
}

func processDT(
		cli *riakcli.Client,
		bt []byte, bucket []byte, key []byte,
		outputChan chan []byte,
		errorChan chan error,
) {
	req := riakprotobuf.DtFetchReq{
		Type: bt,
		Bucket: bucket,
		Key: key,
	}

	err, conn, reqbuf := cli.SendMessage(&req, riakprotobuf.CodeDtFetchReq)
	if err != nil {
		errorChan <- err
		return
	}

	err, headerbuf, responsebuf := cli.ReceiveRawMessage(conn, false)
	if (err != nil) {
		errorChan <- err
		return
	}

	// output the raw messages
	// all messages should be self-decoding

	outputChan <- reqbuf
	outputChan <- headerbuf
	outputChan <- responsebuf
	outputChan <- nil
}
