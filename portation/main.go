package main

import (
	"os"
	"bufio"
	"regexp"
	"log"
	"time"
	// "strings"

	"github.com/cheggaaa/pb"

	riakdata "github.com/likemindnetworks/riakdataportation/data"
	riakcli "github.com/likemindnetworks/riakdataportation/client"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	log.Printf("Riak Data Portation Tool")
	startTime := time.Now()

	var cli = riakcli.
		NewClient("Riak-dev-ELB-749646943.us-east-1.elb.amazonaws.com:8087", 20)

	err := cli.Connect()
	check(err)

	prog := make(chan float64, 100)
	quit := make(chan bool)
	started := false

	bar := pb.New(100)
	bar.SetWidth(80)
	bar.ShowCounters = false
	bar.ShowFinalTime = false

	go func() {
		for {
			pct, more := <-prog

			if !more {
				bar.Set(int(100))
				bar.Finish()
				cli.Close()
				quit <- true
				return
			} else {
				if !started {
					bar.Start()
					started = true
				}

				bar.Set(int(pct * 100))
			}
		}
	}()


	// byteCnt := runExport(cli, prog)
	byteCnt := runImport(cli, prog)

	<-quit

	timeElapsed := time.Since(startTime)

	log.Printf("Bytes processed: %d", byteCnt)
	log.Printf("Time took: %s", timeElapsed)
	log.Printf(
		"Rate: %.4f Kb/s",
		float64(byteCnt) / 100 / float64(timeElapsed / time.Second),
	)
}

func runImport(cli *riakcli.Client, prog chan float64) int {
	log.Printf("Start Importing")

	// bucketOverride := func(b []byte) []byte {
	// 	// version := "mock"

	// 	bucket := string(b)
	// 	colonIndex := strings.Index(bucket, ":")
	// 	atIndex := strings.Index(bucket, "@")

	// 	if (colonIndex < 0) {
	// 		panic("cannot locate : to find app name prefix")
	// 	}

	// 	if atIndex >= 0 && atIndex > colonIndex {
	// 		panic("@ is after :")
	// 	}

	// 	if strings.Count(bucket, ":") > 1 || strings.Count(bucket, "@") > 1 {
	// 		panic("more than 1 : or @")
	// 	}

	// 	// var appNameIndex int

	// 	// if atIndex < 0 {
	// 	// 	appNameIndex = colonIndex
	// 	// } else if (atIndex < colonIndex) {
	// 	// 	appNameIndex = atIndex
	// 	// } else {
	// 	// 	appNameIndex = colonIndex
	// 	// }

	// 	// appName := bucket[0:appNameIndex]
	// 	bucketName := bucket[colonIndex + 1:]

	// 	// return []byte(appName + "@" + version + ":" + bucketName)
	// 	return []byte("teamCS:" + bucketName)
	// }

	f, err := os.Open("./data-export")
	check(err)
	defer f.Close()
	input := bufio.NewReader(f)

	importation := riakdata.NewImport(cli, input, nil);

	cnt, err := importation.Run(prog)
	check(err)

	return cnt
}

func runExport(cli *riakcli.Client, prog chan float64) int {
	log.Printf("Start Exporting")

	f, err := os.Create("./data-export")
	check(err)
	defer f.Close()
	output := bufio.NewWriter(f)

	export := riakdata.NewExport(
		cli,
		func(k []byte) bool {
			match, err := regexp.MatchString("^teamDerek:.*$", string(k))

			return err == nil && match
		},
		[]string{"data"},
		[]string{"sets", "maps", "counters"},
		output,
	);

	cnt, err := export.Run(prog)
	check(err)

	return cnt
}
