# gomr
Map reduce in Go with etcd and S3

## Goal

To make a master-less Map/Reduce framework in Go. etcd is used for co-ordination AWS S3 is used for storing intermittent files, and results. Nobody tells the workers what to do, each worker picks up a pending task and performs it.

I currently use [Disco](http://discoproject.org/). And was thinking if I can make something simpler that does not require a master, or complex setup.

This repository is work in progress.

## Components

1. etcd server/cluster - I have only tested with single etcd server so far.
2. AWS S3 - Currently I only support AWS S3, but its relatively easy to support S3 clones.
3. workers - These wait for some task to be done, and fulfill them accordingly.
4. The user

## Environment 

Install

	go get github.com/turbobytes/gomr

The following environment variables need to be set. Adjust accordingly.

	export S3_BUCKET=gomr  #Existing S3 bucket we will use for this. Its best to use a dedicated bucket.
	export ETCD_SERVERS="http://127.0.0.1:2379" #Comma separated list of etcd servers
	export AWS_REGION=ap-southeast-1 #AWS Region
	export AWS_SECRET_ACCESS_KEY="xxxxxxxxxxxxxx"
	export AWS_ACCESS_KEY_ID="xxxxxxxxxxxx"


## Example

[examples/word_count.go](examples/word_count.go) is an example implementation.
[examples/word_count_execute.go](examples/word_count_execute.go) is the way to submit the task to the cluster.

In one(or multiple) terminal(s) launch the worker process, can be launched anywhere that has access to the etcd cluster and S3.

	go run $GOPATH/src/github.com/turbobytes/gomr/cli/worker.go


Then submit the job.

	cd /tmp
	go build $GOPATH/src/github.com/turbobytes/gomr/examples/word_count.go
	go run $GOPATH/src/github.com/turbobytes/gomr/examples/word_count_execute.go

This should print a job id.
Running word_count_execute.go might take some time depending on your upload bandwidth because it's uploading ~14MB binary to S3. Subsequent runs will be faster because the binary is only uploaded/downloaded if things change.

Check status/fetch result using 

	go run $GOPATH/src/github.com/turbobytes/gomr/cli/fetchresult.go -jobname=ID_FROM_PREVIOUS_STEP -o=/path/to/resultfile

## Project status

This project is in Proof-of-Concept stage. Many failure/retry cases are being ignored currently. Also, currently the whole cluster of workers must target same GOOS/GOARCH .