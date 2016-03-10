package data

import (
	"errors"
	"bufio"
	"log"
	"io"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	riakprotobuf "github.com/likemindnetworks/riakdataportation/protobuf"
	riakcli "github.com/likemindnetworks/riakdataportation/client"
)

var (
	ImportAlreadyRunning = errors.New("Import is already running")
)

type Import struct {
	cli *riakcli.Client
	input *bufio.Reader
	bucketOverrideFn func([]byte) []byte
	wg sync.WaitGroup
	errorChan chan error
}

type reqRespPair struct {
	reqCode byte
	reqBuf []byte
	resCode byte
	resBuf []byte
}

func NewImport(
		cli *riakcli.Client,
		input *bufio.Reader,
		bucketOverrideFn func([]byte) []byte,
) *Import {
	if (bucketOverrideFn == nil) {
		bucketOverrideFn = func(x []byte) []byte { return x }
	}

	return &Import{
		cli: cli,
		input: input,
		bucketOverrideFn: bucketOverrideFn,
	}
}

func (imp *Import) Run() (err error) {

	if imp.errorChan != nil {
		return ImportAlreadyRunning
	}

	imp.errorChan = make(chan error)

	headerBuf := make([]byte, 5, 5)
	totalByteCnt := 0
	quit := make(chan int)

	startTime := time.Now()
	log.Printf("Importing")

	// abort on error
	go func() {
		err, _ = <- imp.errorChan
		quit <- 0
	}()

	for {
		// load a request

		err = imp.fillBuffer(headerBuf)
		if err != nil {
			break
		}

		msgSize, msgReqCode := parseHeader(headerBuf)

		reqData := make([]byte, msgSize, msgSize)
		err = imp.fillBuffer(reqData)
		if err != nil {
			break
		}

		totalByteCnt += 4 + 1 + int(msgSize)

		// load a response

		err = imp.fillBuffer(headerBuf)
		if err != nil {
			break
		}

		msgSize, msgResCode := parseHeader(headerBuf)

		resData := make([]byte, msgSize, msgSize)
		err = imp.fillBuffer(resData)
		if err != nil {
			break
		}

		totalByteCnt += 4 + 1 + int(msgSize)

		// process
		imp.wg.Add(1)
		go imp.processPair(&reqRespPair{
			reqCode: msgReqCode,
			reqBuf: reqData,
			resCode: msgResCode,
			resBuf: resData,
		})
	}

	imp.wg.Wait()

	// no more data after this point
	close(imp.errorChan)

	<-quit

	if err == io.EOF {
		err = nil
	}

	if err == nil {
		log.Printf(
			"%d total bytes imported in %s", totalByteCnt, time.Since(startTime),
		)
	}

	imp.errorChan  = nil
	return
}

func parseHeader(headerBuf []byte) (int, byte) {
	// minus 1 because of the msgcode
	msgSize := -1 + int(headerBuf[0]) << 24 +
		int(headerBuf[1]) << 16 +
		int(headerBuf[2]) << 8 +
		int(headerBuf[3])

	msgCode := headerBuf[4]

	return msgSize, msgCode
}

func (imp *Import) fillBuffer(buf []byte) (err error) {
	c := 0;
	size := len(buf)

	for i := 0; i < size; {
		c, err = imp.input.Read(buf[i:size])
		i += c

		if err != nil {
			break
		}
	}

	return
}

func (imp *Import) processPair(pair *reqRespPair) {
	defer imp.wg.Done()

	var reqMsg, resMsg proto.Message

	// unmarshal req

	switch pair.reqCode {
		case riakprotobuf.CodeRpbGetReq:
			reqMsg = &riakprotobuf.RpbGetReq{}
		case riakprotobuf.CodeDtFetchReq:
			reqMsg = &riakprotobuf.DtFetchReq{}
	}

	err := proto.Unmarshal(pair.reqBuf, reqMsg)
	if err != nil {
		imp.errorChan <- err
		return
	}

	// unmarshal resp

	switch pair.resCode {
		case riakprotobuf.CodeRpbGetResp:
			resMsg = &riakprotobuf.RpbGetResp{}
		case riakprotobuf.CodeDtFetchResp:
			resMsg = &riakprotobuf.DtFetchResp{}
	}

	err = proto.Unmarshal(pair.resBuf, resMsg)
	if err != nil {
		imp.errorChan <- err
		return
	}

	// perform import

	switch pair.resCode {
		case riakprotobuf.CodeRpbGetResp:
			imp.importKV(
				reqMsg.(*riakprotobuf.RpbGetReq),
				resMsg.(*riakprotobuf.RpbGetResp),
			)
		case riakprotobuf.CodeDtFetchResp:
			imp.importDT(
				reqMsg.(*riakprotobuf.DtFetchReq),
				resMsg.(*riakprotobuf.DtFetchResp),
			)
	}
}

func (imp *Import) importKV(
	req *riakprotobuf.RpbGetReq,
	res *riakprotobuf.RpbGetResp,
) {
	newReq := riakprotobuf.RpbPutReq{
		Type: req.Type,
		Bucket: imp.bucketOverrideFn(req.Bucket),
		Key: req.Key,
		Content: res.Content[0],
	}

	err, conn, _ := imp.cli.SendMessage(&newReq, riakprotobuf.CodeRpbPutReq)
	if err != nil {
		imp.errorChan <- err
		return
	}

	newRes := &riakprotobuf.RpbPutResp{}
	err = imp.cli.ReceiveMessage(conn, newRes, false)
	if (err != nil) {
		imp.errorChan <- err
		return
	}
}

func (imp *Import) importDT(
	req *riakprotobuf.DtFetchReq,
	res *riakprotobuf.DtFetchResp,
) {
	dtop := &riakprotobuf.DtOp{}

	switch *res.Type {
	case riakprotobuf.DtFetchResp_COUNTER:
		dtop.CounterOp =
				&riakprotobuf.CounterOp{ Increment: res.Value.CounterValue }
	case riakprotobuf.DtFetchResp_SET:
		dtop.SetOp = &riakprotobuf.SetOp{ Adds: res.Value.SetValue }
	case riakprotobuf.DtFetchResp_MAP:
		dtop.MapOp = createMapOp(res.Value.MapValue)
	}

	w := uint32(1)

	newReq := riakprotobuf.DtUpdateReq{
		Type: req.Type,
		Bucket: imp.bucketOverrideFn(req.Bucket),
		Key: req.Key,
		Op: dtop,
		W: &w,
	}

	err, conn, _ := imp.cli.SendMessage(&newReq, riakprotobuf.CodeDtUpdateReq)
	if err != nil {
		imp.errorChan <- err
		return
	}

	newRes := &riakprotobuf.DtUpdateResp{}
	err = imp.cli.ReceiveMessage(conn, newRes, false)
	if (err != nil) {
		imp.errorChan <- err
		return
	}
}

func createMapOp(entries []*riakprotobuf.MapEntry) *riakprotobuf.MapOp {
	mapEntryCnt := len(entries)
	mapOp := &riakprotobuf.MapOp{}
	mapOp.Updates = make([]*riakprotobuf.MapUpdate, mapEntryCnt, mapEntryCnt)

	for i, mapEntry := range entries {
		mapOp.Updates[i] = &riakprotobuf.MapUpdate{ Field: mapEntry.Field }

		switch *mapEntry.Field.Type {
		case riakprotobuf.MapField_COUNTER:
			mapOp.Updates[i].CounterOp =
					&riakprotobuf.CounterOp{ Increment: mapEntry.CounterValue }
		case riakprotobuf.MapField_SET:
			mapOp.Updates[i].SetOp = &riakprotobuf.SetOp{ Adds: mapEntry.SetValue }
		case riakprotobuf.MapField_REGISTER:
			mapOp.Updates[i].RegisterOp = mapEntry.RegisterValue
		case riakprotobuf.MapField_FLAG:
			var flapOp riakprotobuf.MapUpdate_FlagOp

			if *mapEntry.FlagValue {
				flapOp = riakprotobuf.MapUpdate_ENABLE
			} else {
				flapOp = riakprotobuf.MapUpdate_DISABLE
			}

			mapOp.Updates[i].FlagOp = &flapOp
		case riakprotobuf.MapField_MAP:
			mapOp.Updates[i].MapOp = createMapOp(mapEntry.MapValue)
		}
	}

	return mapOp
}
