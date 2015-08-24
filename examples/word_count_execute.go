package main

import (
	"fmt"
	"github.com/turbobytes/gomr"
	"log"
)

func main() {
	j := &gomr.Job{
		NamePrefix: "WordCount",
		Inputs: []string{
			"https://tools.ietf.org/rfc/rfc4501.txt",
			"https://tools.ietf.org/rfc/rfc2017.txt",
			"https://tools.ietf.org/rfc/rfc2425.txt",
		},
		Partitions: 5,
		S3Bucket:   "", //Its blank... so will be picked up from envoirnment
	}
	name, err := j.Deploy("word_count") //here word_count is the path to the executable we just built
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Deployed:", name)
	fmt.Println("Use the name above to check status/results")
}
