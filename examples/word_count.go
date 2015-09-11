package main

import (
	"bufio"
	"fmt"
	"github.com/turbobytes/gomr"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

//Using FNV-1a non-cryptographic hash function to determine partition for particular key
func hash(s string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	hash := h.Sum32() % uint32(n)
	return int(hash)
}

//Runs once for each user provided input.
//Don't panic, most of the low level things will be moved to library...
func MyMap(input string, job *gomr.Job, logger gomr.Logger) (map[int]string, error) {
	outputs := make(map[int]string)
	var err error
	logger.Info("Rinning map on ", input)
	//Create one TempFile for each partition
	tmpfiles := make([]*os.File, job.Partitions)
	for i, _ := range tmpfiles {
		tmpfiles[i], err = ioutil.TempFile("", "")
		if err != nil {
			return outputs, err
		}
	}
	//Fetch the input url
	resp, err := http.Get(input)
	if err != nil {
		return outputs, err
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	scanner.Split(bufio.ScanWords)
	//Map each instance of a word with 1, use FNV hash to write it to its corresponding partition file
	for scanner.Scan() {
		word := scanner.Text()
		//TODO: Maybe make everything lowercase... and check if its really a "word"
		partition := hash(word, job.Partitions)
		fmt.Fprintf(tmpfiles[partition], "%s\t1\n", word)
	}
	//Close each TempFile and upload to S3
	for i, f := range tmpfiles {
		f.Close()
		newpath, err := job.UploadMapS3(f.Name(), i)
		if err != nil {
			return outputs, err
		}
		outputs[i] = newpath
	}
	//Return the list of S3 files
	return outputs, nil
}

//Run once for map outputs for particular key.
//Don't panic, most of the low level things will be moved to library...
func MyReduce(inputs []string, partition int, job *gomr.Job, logger gomr.Logger) (string, error) {
	f, err := ioutil.TempFile("", "")
	fname := f.Name()
	if err != nil {
		return "", err
	}
	//Download and merge each input into local file
	for _, input := range inputs {
		rd, err := job.FetchInputS3(input)
		if err != nil {
			return "", err
		}
		_, err = io.Copy(f, rd)
		if err != nil {
			return "", err
		}
		rd.Close()
	}
	f.Close()
	//Sort local file by word ... using the sort unix command.. We need lines for each word bunched together
	cmd := exec.Command("sort", fname)
	sorted, err := ioutil.TempFile("", "")
	cmd.Stdout = sorted
	err = cmd.Run()
	if err != nil {
		return "", err
	}
	sortedfname := sorted.Name()
	sorted.Close()
	logger.Info("sorted", sortedfname)
	//The merged file is no longer needed cause we now use the sorted file.
	os.Remove(fname)
	//Create file for final output
	output, err := ioutil.TempFile("", "")
	if err != nil {
		return "", err
	}
	f, err = os.Open(sortedfname)
	if err != nil {
		return "", err
	}
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	word := ""
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		splitted := strings.Split(line, "\t")
		if splitted[0] != word {
			//New word detected, yield previous word
			if count > 0 {
				fmt.Fprintf(output, "%s\t%d\n", word, count)
			}
			word = splitted[0]
			count, err = strconv.Atoi(splitted[1])
			if err != nil {
				return "", err
			}
		} else {
			count++
		}
	}
	//Yield last word
	if count > 0 {
		fmt.Fprintf(output, "%s\t%d\n", word, count)
	}
	f.Close()
	os.Remove(sortedfname)
	outname := output.Name()
	output.Close()
	//Upload output to S3 and return the key
	return job.UploadResultS3(outname)
}

func main() {
	//Boilerplate to actualy execute the job on a worker
	jobname := os.Args[1]
	w := &gomr.Worker{
		Map:    MyMap,
		Reduce: MyReduce,
	}
	w.Execute(jobname)
}
