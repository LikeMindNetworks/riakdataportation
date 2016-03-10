package main

import (
	"os"
	"bufio"
	"regexp"
	"log"
	"fmt"
	"time"
	"flag"
	"strings"
	"path"

	"github.com/cheggaaa/pb"

	riakdata "github.com/likemindnetworks/riakdataportation/data"
	riakcli "github.com/likemindnetworks/riakdataportation/client"
)

const Version = "1.0.0"

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func printHelp() {
	fmt.Println("Usage: portation <export|import> <riak host with port>")
	flag.PrintDefaults()
}

func main() {
	log.Printf("Riak Data Portation Tool")

	var (
		numConnection = flag.Int("c", 20, "number of connections")
		appName = flag.String("a", "", "app name")
		dataVersion = flag.String("d", "", "data version")
		inputFile = flag.String("i", "", "input file for import")
		outputDir = flag.String("o", ".", "output folder for export result")
		printVersion = flag.Bool("v", false, "print version")
	)

	flag.Parse()

	if *printVersion {
		fmt.Println(Version)
		return
	}

	if len(flag.Args()) != 2 {
		printHelp()
		return
	}

	action := flag.Args()[0]
	host := flag.Args()[1]

	if action != "import" && action != "export" {
		printHelp()
		return
	}

	if strings.ContainsAny(*appName, "@:") ||
			strings.ContainsAny(*dataVersion, "@:") {
		panic("app name and data version should never contain @ or :")
	}

	// warn when try to override appName during import
	if action == "import" && len(*appName) > 0 {
		fmt.Println("Trying to override app name during import. Are you sure?");
		fmt.Printf("Type the full app name (%s) to confirm: ", *appName)

		readConfirm := bufio.NewReader(os.Stdin)
		text, _ := readConfirm.ReadString('\n')

		if text[:len(text) - 1] != *appName {
			panic("Not Confirmed. Abort import.")
		}
	}

	// start
	startTime := time.Now()

	var cli = riakcli.NewClient(host, *numConnection)

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

	var byteCnt int

	switch action {
	case "import":
		byteCnt = runImport(cli, appName, dataVersion, inputFile, prog)
	case "export":
		byteCnt = runExport(cli, appName, dataVersion, outputDir, prog)
	}

	<-quit

	timeElapsed := time.Since(startTime)

	log.Printf("Bytes processed: %d", byteCnt)
	log.Printf("Time took: %s", timeElapsed)
	log.Printf(
		"Rate: %.4f Kb/s",
		float64(byteCnt) / 100 / float64(timeElapsed / time.Second),
	)
}

func runImport(
	cli *riakcli.Client,
	appName *string,
	dataVersion *string,
	inputFile *string,
	prog chan float64,
) int {
	log.Printf("Start Importing")

	bucketOverride := func(b []byte) []byte {
		bucket := string(b)
		colonIndex := strings.Index(bucket, ":")
		atIndex := strings.Index(bucket, "@")

		if (colonIndex < 0) {
			panic("cannot locate : to find app name prefix")
		}

		if atIndex >= 0 && atIndex > colonIndex {
			panic("@ is after :")
		}

		if atIndex + 1 == colonIndex {
			panic("Empty data version")
		}

		if strings.Count(bucket, ":") > 1 || strings.Count(bucket, "@") > 1 {
			panic("more than 1 : or @")
		}

		var origAppName string
		var origDataVersion string
		var bucketName string
		var resBucket string

		if atIndex >= 0 {
			// has data version in backup
			origAppName = bucket[0:atIndex]
			origDataVersion = bucket[atIndex + 1:colonIndex]
		} else {
			// no data version in backup
			origAppName = bucket[0:colonIndex]
		}

		bucketName = bucket[colonIndex + 1:]

		if len(*appName) == 0 {
			// no override for app name use original
			appName = &origAppName
		}
		resBucket = *appName

		if len(*dataVersion) > 0 {
			// has override for data version
			resBucket += "@" + *dataVersion
		} else if len(origDataVersion) > 0 {
			// has original data version either
			resBucket += "@" + origDataVersion
		}

		resBucket += ":" + bucketName

		return []byte(resBucket)
	}

	if len(*inputFile) == 0 {
		panic("missing input file")
	}

	f, err := os.Open(*inputFile)
	check(err)
	defer f.Close()
	input := bufio.NewReader(f)

	importation := riakdata.NewImport(cli, input, bucketOverride);

	cnt, err := importation.Run(prog)
	check(err)

	return cnt
}

func runExport(
	cli *riakcli.Client,
	appName *string,
	dataVersion *string,
	outputDir *string,
	prog chan float64,
) int {
	log.Printf("Start Exporting")

	if len(*appName) == 0 {
		panic("missing app name")
	}

	prefix := *appName

	if len(*dataVersion) > 0 {
		// has data version
		prefix += "@" + *dataVersion
	}

	f, err := os.Create(path.Join(
		*outputDir,
		prefix + "." + time.Now().Format(time.RFC3339) + ".bin",
	))
	check(err)
	defer f.Close()
	output := bufio.NewWriter(f)

	export := riakdata.NewExport(
		cli,
		func(k []byte) bool {
			match, err := regexp.MatchString("^" + prefix + ":.*$", string(k))

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
