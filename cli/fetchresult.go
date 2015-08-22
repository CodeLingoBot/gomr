package main

import (
	"flag"
	"fmt"
	"github.com/turbobytes/gomr"
	"log"
)

func main() {
	var jobname, fname string
	flag.StringVar(&jobname, "jobname", "", "Jobname - the one returned when submitting the job")
	flag.StringVar(&fname, "o", "", "Where to store the results")
	flag.Parse()
	log.Println("Fetching job data...")
	if jobname == "" {
		log.Fatal()
	}
	job, err := gomr.FetchJob(jobname)
	if err != nil {
		log.Fatal(err)
	}
	if job.Status != gomr.StatusDone {
		log.Fatal("Job not finished")
	} else {
		fmt.Println("Job finished, fetching results")
	}
	err = job.FetchResults(fname)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Results fetched:", fname)

}
