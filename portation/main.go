package main

import (
	"os"
	"bufio"
	"regexp"

	riakdata "github.com/likemindnetworks/riakdataportation/data"
	riakcli "github.com/likemindnetworks/riakdataportation/client"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
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
			match, err := regexp.MatchString("^teamLMN:.*$", string(k))

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
