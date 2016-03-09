package data

import (
	"log"

	riakprotobuf "github.com/likemindnetworks/riakdataportation/protobuf"
	riakcli "github.com/likemindnetworks/riakdataportation/client"
)

func Export(
		cli *riakcli.Client,
		filterFn func([]byte) bool,
		bucketTypes []string,
		dtBucketTypes []string,
) (err error) {
	for _, bt := range bucketTypes {
		err = processBucketType(cli, filterFn, []byte(bt), false)

		if (err != nil) {
			break
		}
	}

	for _, bt := range dtBucketTypes {
		err = processBucketType(cli, filterFn, []byte(bt), true)

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
) (err error) {
	isStreaming := true
	req := riakprotobuf.RpbListBucketsReq{ Type: bt, Stream: &isStreaming }

	err, conn := cli.SendMessage(&req, riakprotobuf.CodeRpbListBucketsReq)
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

	log.Printf("%d buckets retrieved in type [%s] %d to be processed",
		len(buckets),
		bt,
		len(filteredBuckets))

	// process and wait
	resultChan := make(chan int)

	for _, b := range filteredBuckets {
		go processBucket(cli, bt, b, isRiakDt, resultChan)
	}

	for range filteredBuckets {
		log.Printf("%d keys retrieved", <-resultChan)
	}

	return
}

func processBucket(
		cli *riakcli.Client,
		bt []byte, bucket []byte,
		isRiakDt bool,
		resultChan chan int,
) {
	req := riakprotobuf.RpbListKeysReq{ Type: bt, Bucket: bucket }

	err, conn := cli.SendMessage(&req, riakprotobuf.CodeRpbListKeysReq)
	if err != nil {
		panic(err)
	}

	var (
		partial *riakprotobuf.RpbListKeysResp
		keys [][]byte
	)

	for {
		partial = &riakprotobuf.RpbListKeysResp{}
		err = cli.ReceiveMessage(conn, partial, true)
		if (err != nil) {
			panic(err)
		}

		keys = append(keys, partial.Keys...)

		if partial.Done != nil {
			break;
		}
	}

	cli.ReleaseConnection(conn)

	// process and wait
	bucketResultChan := make(chan int)

	for _, key := range keys {
		if isRiakDt {
			go processDT(cli, bt, bucket, key, bucketResultChan)
		} else {
			go processKV(cli, bt, bucket, key, bucketResultChan)
		}
	}

	for range keys {
		<-bucketResultChan
	}

	resultChan <- len(keys);
}

func processKV(
	cli *riakcli.Client,
	bt []byte, bucket []byte, key []byte,
	resultChan chan int,
) {
	req := riakprotobuf.RpbGetReq{
		Type: bt,
		Bucket: bucket,
		Key: key,
	}

	err, conn := cli.SendMessage(&req, riakprotobuf.CodeRpbGetReq)
	if err != nil {
		panic(err)
	}

	resp := &riakprotobuf.RpbGetResp{}
	err = cli.ReceiveMessage(conn, resp, false)
	if (err != nil) {
		panic(err)
	}

	log.Printf("%s", resp.Content)

	resultChan <- 1;
}

func processDT(
	cli *riakcli.Client,
	bt []byte, bucket []byte, key []byte,
	resultChan chan int,
) {
	req := riakprotobuf.DtFetchReq{
		Type: bt,
		Bucket: bucket,
		Key: key,
	}

	err, conn := cli.SendMessage(&req, riakprotobuf.CodeDtFetchReq)
	if err != nil {
		panic(err)
	}

	resp := &riakprotobuf.DtFetchResp{}
	err = cli.ReceiveMessage(conn, resp, false)
	if (err != nil) {
		panic(err)
	}

	log.Printf("%s", resp.Value)

	resultChan <- 1;
}
