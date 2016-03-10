package main

import (
	"os"
	"bufio"
	"regexp"
	"log"

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
	// testExport()
	testImport()
}

func testImport() {
	var cli = riakcli.
		NewClient("Riak-dev-ELB-749646943.us-east-1.elb.amazonaws.com:8087", 10)

	err := cli.Connect()
	check(err)

	f, err := os.Open("./data-export")
	check(err)
	defer f.Close()
	input := bufio.NewReader(f)

	importation := riakdata.NewImport(cli, input, nil);

	err = importation.Run()
	check(err)

	cli.Close()
}

func testExport() {
	var cli = riakcli.
		NewClient("Riak-dev-ELB-749646943.us-east-1.elb.amazonaws.com:8087", 10)

	err := cli.Connect()
	check(err)

	f, err := os.Create("./data-export")
	check(err)
	defer f.Close()
	output := bufio.NewWriter(f)

	export := riakdata.NewExport(
		cli,
		func(k []byte) bool {
			match, err := regexp.MatchString("^teamCS:.*$", string(k))

			return err == nil && match
		},
		[]string{"data"},
		[]string{"sets", "maps", "counters"},
		output,
	);

	err = export.Run()
	check(err)

	cli.Close()
}
