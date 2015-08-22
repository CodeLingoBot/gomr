package main

import (
	"github.com/turbobytes/gomr"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

func execute(binpath, jobname, bucketname string) {
	log.Println("TASK", binpath, jobname, bucketname)
	dir := os.TempDir() + "/gomrbin"
	//log.Println(dir)
	_, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(dir, 0776)
			if err != nil {
				log.Println(err)
				return
			}
		} else {
			log.Println(err)
			return
		}
	}
	log.Println("Temp dir:", dir)
	//Check if binary exists
	bin := dir + "/" + strings.Split(binpath, "/")[1]
	log.Println(bin)
	_, err = os.Stat(bin)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("Downloading binary from S3")
			env := gomr.NewEnvironment()
			s3bucket, err := env.GetS3Bucket(bucketname)
			if err != nil {
				log.Println(err)
				return
			}
			data, err := s3bucket.Get(binpath)
			if err != nil {
				log.Println(err)
				return
			}
			f, err := os.Create(bin)
			if err != nil {
				log.Println(err)
				return
			}
			_, err = f.Write(data)
			if err != nil {
				log.Println(err)
				f.Close()
				os.Remove(bin)
				return
			}
			err = f.Chmod(100)
			if err != nil {
				log.Println(err)
				f.Close()
				os.Remove(bin)
				return
			}
			f.Close()
		} else {
			log.Println(err)
			return
		}
	}
	//Now execute...
	cmd := exec.Command(bin, jobname)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Println(cmd.Run())
}

func main() {
	//Run in infinite loop...
	for {
		tasks, err := gomr.GetIncompleteJobs()
		if err != nil {
			log.Println(err)
		} else {
			if len(tasks) == 0 {
				log.Println("Nothing to do... boring..")
			} else {
				for _, task := range tasks {
					execute(task.Binary, task.JobName, task.BucketName)
				}
			}
		}
		time.Sleep(time.Second * 5)
	}
}
