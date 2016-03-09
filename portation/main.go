package main

import (
	"log"
	"time"
	"regexp"

	riakdata "likemindnetworks.com/riak/data"
	riakcli "likemindnetworks.com/riak/client"
)

func main() {
	var cli = riakcli.
		NewClient("Riak-dev-ELB-749646943.us-east-1.elb.amazonaws.com:8087", 20)

	cli.Connect()
	startTime := time.Now()

	err := riakdata.Export(
		cli,
		func(k []byte) bool {
			match, err := regexp.MatchString("^teamCS:.*$", string(k))

			return err == nil && match
		},
		[]string{"data"},
		[]string{"sets", "maps", "counters"},
	);

	if (err != nil) {
		panic(err)
	}

	elapsedTime := time.Since(startTime)
	cli.Close()

	log.Println()
	log.Printf("Took %s", elapsedTime)
}
