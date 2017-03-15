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

	// riakprotobuf "github.com/likemindnetworks/riakdataportation/protobuf"
)

const Version = "1.1.2"

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
	fmt.Println("Riak Data Portation Tool")

	var (
		numConnection = flag.Int("c", 20, "number of connections")
		appName = flag.String("a", "", "app name")
		dataVersion = flag.String("d", "", "data version")
		inputFile = flag.String("i", "", "input file for import")
		outputDir = flag.String("o", ".", "output folder for export result")
		printVersion = flag.Bool("v", false, "print version")
		verbose = flag.Bool("verbose", false, "print debug statements")

		dryrunImport = flag.Bool("dryrun-import", false, "dry run import")
		forceOriVclock = flag.Bool("fov", false, "force to use original vclock for import")
		noVclock = flag.Bool("nv", false, "force to use no vclock for import")
		deleteBeforeImport = flag.Bool(
			"delete-before-import", false, "delete values before import",
		)

		deleteAfterExport = flag.Bool(
			"delete-after-export", false, "delete values after export (wipe db)",
		)
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

	if *forceOriVclock && *noVclock {
		panic("flag 'fov' and 'nv' cannot be used together")
	}

	// printing flags
	fmt.Printf("Task: %s\n", action)
	fmt.Printf("Host: %s\n", host)
	fmt.Printf("Verbose: %t\n", *verbose)
	fmt.Printf("App Name: %s\n", *appName)
	fmt.Printf("Data Version: @%s\n", *dataVersion)

	if action == "import" {
		fmt.Printf("Input File: %s\n", *inputFile)
		fmt.Printf("Dry Run: %t\n", *dryrunImport)
		fmt.Printf("Delete Before Import: %t\n", *deleteBeforeImport)
	} else {
		fmt.Printf("Output Directory: %s\n", *outputDir)
		fmt.Printf("Wipe DB(-delete-after-export): %t\n", *deleteAfterExport)
	}
	// end printing flags

	// confirmation

	needToConfirm := false

	// warn when try to override appName during import
	if action == "import" && len(*appName) > 0 {
		fmt.Println("Trying to override app name during import. Are you sure?")
		needToConfirm = true
	}

	if action == "export" {
		if len(*appName) == 0 {
			panic("must specify app name")
		}

		if *deleteAfterExport {
			fmt.Printf(
				"Trying to wipe data for %s@%s. Are you sure?\n",
				*appName, *dataVersion,
			)
			needToConfirm = true
		}
	}

	if needToConfirm {
		fmt.Printf("Type the full app name (%s) to confirm: ", *appName)

		readConfirm := bufio.NewReader(os.Stdin)
		text, _ := readConfirm.ReadString('\n')

		if text[:len(text) - 1] != *appName {
			panic("Not Confirmed. Abort import.")
		}
	}

	// start
	startTime := time.Now()

	var cli = riakcli.NewClient(host, *numConnection, *verbose)

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

	if runTest(cli) {
		switch action {
		case "import":
			byteCnt = runImport(
				cli, appName, dataVersion, inputFile,
				*dryrunImport, *deleteBeforeImport, *forceOriVclock, *noVclock, prog,
			)
		case "export":
			byteCnt = runExport(
				cli, appName, dataVersion, outputDir,
				*deleteAfterExport, prog,
			)
		}
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

func runTest(cli *riakcli.Client) (res bool) {
	res = true
	return
}

func runImport(
	cli *riakcli.Client,
	appName *string,
	dataVersion *string,
	inputFile *string,
	isDryRun bool,
	isDeleteFirst bool,
	isForceOriVclock bool,
	isNoVclock bool,
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

	importation := riakdata.NewImport(
		cli, input, bucketOverride, isDryRun, isDeleteFirst, isForceOriVclock, isNoVclock,
	);

	cnt, err := importation.Run(prog)
	check(err)

	return cnt
}

func runExport(
	cli *riakcli.Client,
	appName *string,
	dataVersion *string,
	outputDir *string,
	isDeleteAfter bool,
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
		isDeleteAfter,
	);

	cnt, err := export.Run(prog)
	check(err)

	return cnt
}
