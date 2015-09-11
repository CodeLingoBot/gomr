package gomr

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"github.com/satori/go.uuid"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	StatusInitialized = 0
	StatusMapStage    = 1
	StatusReduceStage = 2
	StatusFail        = 3
	StatusDone        = 4
)

//Sortable list
type Joblist []*Job

//Sort interface
func (a Joblist) Len() int           { return len(a) }
func (a Joblist) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Joblist) Less(i, j int) bool { return a[i].CreatedAt.Before(a[j].CreatedAt) }

type Worker struct {
	Map    func(input string, job *Job) (map[int]string, error)
	Reduce func(inputs []string, partition int, job *Job) (string, error)
}

func gzipfile(fname string, output io.WriteCloser) error {
	input, err := os.Open(fname)
	if err != nil {
		return err
	}
	writer := gzip.NewWriter(output)
	_, err = io.Copy(writer, input)
	if err != nil {
		return err
	}
	writer.Close()
	output.Close()
	input.Close()
	//os.Remove(fname)
	return nil
}

//Tracks progress of each stage
type StageProgress struct {
	Total   int //Total tasks for this stage
	Waiting int //Tasks waiting to be allocated
	Running int //Tasks currently running
	Done    int //Tasks finished
	Failed  int //Tasks failed
}

func (p *StageProgress) update(resp *etcd.Response) error {
	for _, node := range resp.Node.Nodes {
		for _, subnode := range node.Nodes {
			if strings.HasSuffix(subnode.Key, "status") {
				status, err := strconv.Atoi(subnode.Value)
				if err != nil {
					return err
				}
				//log.Println(status)
				switch status {
				case StatusInitialized:
					p.Running++
				case StatusDone:
					p.Done++
				case StatusFail:
					p.Failed++
				}
			}
			//log.Println(subnode.Key)
		}
	}
	p.Waiting = p.Total - (p.Done + p.Failed + p.Running)
	return nil
}

type Job struct {
	Params         map[string]interface{} //Arbitary Kv - must be json encodable
	NamePrefix     string                 //Optional - single word, only alphanumeric
	Name           string                 //NamePrefix + some uuid. Generated automatically
	Inputs         []string               //List of inputs, this should be something that makes sense to the map stage
	Partitions     int                    //Number of partitions desired... this is sent to map/reduce stage and can be ignored.
	Status         int                    //StatusInitialized or StatusMapStage or StatusReduceStage or StatusFail or StatusDone
	Results        []string               //Populated once job is complete
	S3Bucket       string                 //S3 Bucket name
	S3Prefix       string                 // /Job.Name/ gets appended
	BinaryFile     string                 //Path to binary file - auto created
	NumMaps        int                    //Number of inputs for map stage a.k.a. len(Inputs)
	NumReduces     int                    //Number of inputs for reduce stage - populated once all map have finished
	CreatedAt      time.Time              //Timestamp of when the Job was initially submitted - used for sorting
	MapProgress    *StageProgress
	ReduceProgress *StageProgress
}

//Fetch all jobs, but only their status is populated, this is done to not access S3 where the real initial job is stored
func FetchAllJobs() (Joblist, error) {
	jobs := []*Job{}
	env := NewEnvironment()
	cl := env.GetEtcdClient()
	defer cl.Close()
	resp, err := cl.Get("/gomr/", false, true)
	if err != nil {
		return jobs, err
	}
	for _, node := range resp.Node.Nodes {
		splitted := strings.Split(node.Key, "/")
		if len(splitted) == 3 {
			j := &Job{Name: splitted[2]}
			err = j.UpdateStatus()
			if err != nil {
				return jobs, err
			}
			jobs = append(jobs, j)
		}
	}
	sortedjobs := Joblist(jobs)
	sort.Sort(sort.Reverse(sortedjobs))
	return sortedjobs, nil
}

func FetchJob(jobname string) (*Job, error) {
	j := &Job{}
	env := NewEnvironment()
	//Get job data
	cl := env.GetEtcdClient()
	defer cl.Close()
	eprefix := "/gomr/" + jobname + "/"

	resp, err := cl.Get(eprefix+"s3bucket", false, true)
	if err != nil {
		return nil, err
	}
	s3bucket := resp.Node.Value
	log.Println("s3bucket", s3bucket)
	resp, err = cl.Get(eprefix+"s3prefix", false, true)
	if err != nil {
		return nil, err
	}
	s3prefix := resp.Node.Value
	log.Println("s3prefix", s3prefix)
	bucket, err := env.GetS3Bucket(s3bucket)
	if err != nil {
		return nil, err
	}
	data, err := bucket.Get(s3prefix + "jobdata.json")
	if err != nil {
		return nil, err
	}
	log.Println("jobdata", string(data))

	err = json.Unmarshal(data, j)
	if err != nil {
		return nil, err
	}
	err = j.UpdateStatus()
	if err != nil {
		return nil, err
	}
	return j, nil
}

//Update Job Status
func (j *Job) UpdateStatus() error {
	//Populate status info
	env := NewEnvironment()
	//Get job data
	cl := env.GetEtcdClient()
	defer cl.Close()
	eprefix := "/gomr/" + j.Name + "/"
	resp, err := cl.Get(eprefix+"status", false, false)
	if err != nil {
		return err
	}
	status, err := strconv.Atoi(resp.Node.Value)
	if err != nil {
		return err
	}
	j.Status = status
	//Populate NumMaps and NumReduces
	resp, err = cl.Get(eprefix+"nummaps", false, false)
	if err != nil {
		return err
	}
	j.NumMaps, err = strconv.Atoi(resp.Node.Value)
	if err != nil {
		return err
	}
	//Populate NumMaps and NumReduces
	resp, err = cl.Get(eprefix+"numreduces", false, false)
	if err != nil {
		return err
	}
	j.NumReduces, err = strconv.Atoi(resp.Node.Value)
	if err != nil {
		return err
	}
	//Populate StageProgress
	j.MapProgress = &StageProgress{Total: j.NumMaps}
	j.ReduceProgress = &StageProgress{Total: j.NumReduces}

	resp, err = cl.Get(eprefix+"map", false, true)
	if err != nil {
		return err
	}
	err = j.MapProgress.update(resp)
	if err != nil {
		return err
	}

	resp, err = cl.Get(eprefix+"reduce", false, true)
	if err != nil {
		return err
	}
	err = j.ReduceProgress.update(resp)
	if err != nil {
		return err
	}

	//Update CreatedAt, in-case this is not the full S3 json
	resp, err = cl.Get(eprefix+"createdat", false, false)
	if err != nil {
		return err
	}
	j.CreatedAt, err = time.Parse(time.RFC3339, resp.Node.Value)
	if err != nil {
		return err
	}

	//Populate results
	if status == StatusDone {
		resp, err = cl.Get(eprefix+"results", false, false)
		for _, node := range resp.Node.Nodes {
			//log.Println(node.Key, node.Value)
			j.Results = append(j.Results, node.Value)
		}
	}
	return nil
}

//Fetch results of this job into localfile
func (j *Job) FetchResults(fname string) error {
	env := NewEnvironment()
	bucket, err := env.GetS3Bucket(j.S3Bucket)
	if err != nil {
		return err
	}
	f, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, result := range j.Results {
		log.Println("Fetching:", result)
		rd, err := bucket.GetReader(result)
		if err != nil {
			return err
		}
		gzrd, err := gzip.NewReader(rd)
		if err != nil {
			return err
		}
		_, err = io.Copy(f, gzrd)
		gzrd.Close()
		rd.Close()
	}
	return nil
}

type Task struct {
	Binary     string
	JobName    string
	BucketName string
}

//Return list of jobnames that arent complete...
func GetIncompleteJobs() ([]*Task, error) {
	jobs := []*Task{}
	env := NewEnvironment()
	cl := env.GetEtcdClient()
	defer cl.Close()
	resp, err := cl.Get("/gomr", false, false)
	if err != nil {
		return jobs, err
	}
	for _, node := range resp.Node.Nodes {
		//Check if this job is finished or not...
		resp, err = cl.Get(node.Key+"/status", false, false)
		if err != nil {
			return jobs, err
		}
		status, err := strconv.Atoi(resp.Node.Value)
		if err != nil {
			return jobs, err
		}
		if status != StatusDone {
			splitted := strings.Split(node.Key, "/")
			key := splitted[len(splitted)-1]
			resp, err = cl.Get(node.Key+"/bin", false, false)
			if err != nil {
				return jobs, err
			}
			resp1, err := cl.Get(node.Key+"/s3bucket", false, false)
			if err != nil {
				return jobs, err
			}
			jobs = append(jobs, &Task{resp.Node.Value, key, resp1.Node.Value})
		}
	}
	return jobs, nil
}

func sha256sum(binfile string) (string, error) {
	f, err := os.Open(binfile)
	if err != nil {
		return "", nil
	}
	defer f.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func uploads3file(path, file, contenttype string, bucket *s3.Bucket) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	err = bucket.PutReader(path, f, info.Size(), contenttype, s3.Private)
	if err != nil {
		return err
	}
	return nil
}

//Uploads only if the given key does not exist
func uploads3fileifnotexists(binpath, binfile, contenttype string, bucket *s3.Bucket) error {
	k, _ := bucket.GetKey(binpath)
	if k == nil {
		//Binary does not exist on s3.. gzip and upload it now...
		finalfile, err := gziptotempfile(binfile)
		if err != nil {
			return err
		}
		return uploads3file(binpath, finalfile, "application/x-gzip", bucket)
	}
	return nil
}

//Gzip given filename, return path to gzipped file
func gziptotempfile(fname string) (string, error) {
	gzfile, err := ioutil.TempFile("", "")
	if err != nil {
		return "", err
	}
	err = gzipfile(fname, gzfile)
	if err != nil {
		return "", err
	}
	return gzfile.Name(), nil
}

//Compress and upload output to given path. deleting the source file
func (j *Job) uploadoutput(fname string, path string) (string, error) {
	env := NewEnvironment()
	bucket, err := env.GetS3Bucket(j.S3Bucket)
	if err != nil {
		return "", err
	}
	//Gzip
	gzfile, err := ioutil.TempFile("", "")
	if err != nil {
		return "", err
	}
	err = gzipfile(fname, gzfile)
	if err != nil {
		return "", err
	}
	os.Remove(fname)
	err = uploads3file(path, gzfile.Name(), "application/x-gzip", bucket)
	if err != nil {
		return "", err
	}
	//Delete the temporary file...
	os.Remove(fname)
	return path, nil
}

//Helper function to upload map output to s3
func (j *Job) UploadMapS3(fname string, partition int) (string, error) {
	return j.uploadoutput(fname, fmt.Sprintf("%smaps/%d-%s", j.S3Prefix, partition, uuid.NewV4()))
}

//Helper function to upload reduce output to s3
func (j *Job) UploadResultS3(fname string) (string, error) {
	return j.uploadoutput(fname, fmt.Sprintf("%sresults/%s", j.S3Prefix, uuid.NewV4()))
}

func (j *Job) FetchInputS3(path string) (rc io.ReadCloser, err error) {
	env := NewEnvironment()
	bucket, err := env.GetS3Bucket(j.S3Bucket)
	if err != nil {
		return nil, err
	}
	rd, err := bucket.GetReader(path)
	if err != nil {
		return nil, err
	}
	return gzip.NewReader(rd)
}

//Deploy job things to S3 and initialize etcd keys.
func (j *Job) Deploy(binfile string) (string, error) {
	u := uuid.NewV4()
	prefix := ""
	if j.NamePrefix != "" {
		prefix = j.NamePrefix + "-"
	}
	j.Name = fmt.Sprintf("%s%s", prefix, u)
	//Get name of binary file
	var err error
	j.BinaryFile, err = sha256sum(binfile)
	if err != nil {
		return "", err
	}

	//Upload binary to s3...
	env := NewEnvironment()
	if j.S3Bucket == "" {
		j.S3Bucket = env.S3_BUCKET
	}
	bucket, err := env.GetS3Bucket(j.S3Bucket)
	if err != nil {
		return "", err
	}
	binpath := "bin/" + j.BinaryFile
	//Check if file already exists... without downloading
	err = uploads3fileifnotexists(binpath, binfile, "application/octet-stream", bucket)
	if err != nil {
		return "", err
	}

	//Insert timestamp
	j.CreatedAt = time.Now()

	//Calculate NumMaps
	j.NumMaps = len(j.Inputs)

	//Upload job json to s3...
	//TODO: Gzip before upload...
	j.S3Prefix = fmt.Sprintf("%s/%s/", j.S3Prefix, j.Name)
	b, err := json.Marshal(j)
	if err != nil {
		return "", err
	}
	err = bucket.Put(j.S3Prefix+"jobdata.json", b, "application/json", s3.Private)
	if err != nil {
		return "", err
	}

	//TODO: Create etcd keys...

	cl := env.GetEtcdClient()
	defer cl.Close()
	eprefix := "/gomr/" + j.Name + "/"
	//Create directory
	_, err = cl.CreateDir(eprefix, 0)
	if err != nil {
		return "", err
	}
	//Add job metadata
	_, err = cl.Create(eprefix+"status", strconv.Itoa(j.Status), 0)
	if err != nil {
		return "", err
	}

	//Add job binary info
	_, err = cl.Create(eprefix+"bin", binpath, 0)
	if err != nil {
		return "", err
	}

	//S3 bucket we are using
	_, err = cl.Create(eprefix+"s3bucket", j.S3Bucket, 0)
	if err != nil {
		return "", err
	}

	//S3 prefix
	_, err = cl.Create(eprefix+"s3prefix", j.S3Prefix, 0)
	if err != nil {
		return "", err
	}

	//Store CreatedAt
	_, err = cl.Create(eprefix+"createdat", j.CreatedAt.Format(time.RFC3339), 0)
	if err != nil {
		return "", err
	}

	//Store NumMaps
	_, err = cl.Create(eprefix+"nummaps", strconv.Itoa(j.NumMaps), 0)
	if err != nil {
		return "", err
	}

	//Store NumReduces - this will be 0 for now
	_, err = cl.Create(eprefix+"numreduces", strconv.Itoa(j.NumReduces), 0)
	if err != nil {
		return "", err
	}

	//Create directory for maps
	_, err = cl.CreateDir(eprefix+"map/", 0)
	if err != nil {
		return "", err
	}

	//Create directory for reduce
	_, err = cl.CreateDir(eprefix+"reduce/", 0)
	if err != nil {
		return "", err
	}

	//Create directory for results
	_, err = cl.CreateDir(eprefix+"results/", 0)
	if err != nil {
		return "", err
	}

	/*
		//Create individual map tasks
		for i, input := range j.Inputs {
			//Create directory for map task
			_, err = cl.CreateDir(eprefix+"map/"+strconv.Itoa(i)+"/", 0)
			if err != nil {
				return "", err
			}
			//Write input url
			_, err = cl.Create(eprefix+"map/"+strconv.Itoa(i)+"/"+"input", input, 0)
			if err != nil {
				return "", err
			}
		}
	*/
	j.Status = StatusInitialized
	return j.Name, err
}

//Fetch a task to do and run it
func (w *Worker) Execute(jobname string) {
	log.Println("Doing...", jobname)
	log.Println("Looking for map jobs...")
	env := NewEnvironment()
	//Get job data
	cl := env.GetEtcdClient()
	defer cl.Close()
	eprefix := "/gomr/" + jobname + "/"
	//Check if job is already completed... if so then abort...
	resp, err := cl.Get(eprefix+"status", false, false)
	if err != nil {
		log.Fatal(err)
	}
	status, err := strconv.Atoi(resp.Node.Value)
	if err != nil {
		log.Fatal(err)
	}
	if status == StatusDone {
		log.Fatal("Job is already done...")
	}

	resp, err = cl.Get(eprefix+"s3bucket", false, true)
	if err != nil {
		log.Fatal(err)
	}
	s3bucket := resp.Node.Value
	log.Println("s3bucket", s3bucket)
	resp, err = cl.Get(eprefix+"s3prefix", false, true)
	if err != nil {
		log.Fatal(err)
	}
	s3prefix := resp.Node.Value
	log.Println("s3prefix", s3prefix)
	bucket, err := env.GetS3Bucket(s3bucket)
	if err != nil {
		log.Fatal(err)
	}
	data, err := bucket.Get(s3prefix + "jobdata.json")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("jobdata", string(data))
	j := &Job{}
	err = json.Unmarshal(data, j)
	if err != nil {
		log.Fatal(err)
	}

	//Check if any map tasks need dooing...
	for i, input := range j.Inputs {
		//Check if i exists... if not create it and we will process it.
		_, err = cl.CreateDir(eprefix+"map/"+strconv.Itoa(i)+"/", 0)
		if err == nil {
			//Means we could create it, nobody else has it
			log.Println("Aquired lock for map task ", i)
			//Update Status - we are obviously in map phase
			_, err = cl.Update(eprefix+"status", strconv.Itoa(StatusMapStage), 0)
			if err != nil {
				log.Fatal(err)
			}

			//Create task status
			_, err = cl.Create(eprefix+"map/"+strconv.Itoa(i)+"/"+"status", strconv.Itoa(StatusInitialized), 0)
			if err != nil {
				log.Fatal(err)
			}

			//Write input url
			_, err = cl.Create(eprefix+"map/"+strconv.Itoa(i)+"/"+"input", input, 0)
			if err != nil {
				log.Fatal(err)
			}
			//Start map task
			log.Println("Starting map task", i)
			outputs, err := w.Map(input, j)
			log.Println(outputs, err)
			if err != nil {
				//TODO: Have retries for failures...
				log.Fatal(err)
			}
			//Store outputs in etcd
			_, err = cl.CreateDir(eprefix+"map/"+strconv.Itoa(i)+"/outputs/", 0)
			if err != nil {
				log.Fatal(err)
			}
			for idx, output := range outputs {
				_, err = cl.Create(eprefix+"map/"+strconv.Itoa(i)+"/"+"outputs/"+strconv.Itoa(idx), output, 0)
				if err != nil {
					log.Fatal(err)
				}
			}
			//Mark as done
			_, err = cl.Update(eprefix+"map/"+strconv.Itoa(i)+"/"+"status", strconv.Itoa(StatusDone), 0)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	//Check if map phase has finished....
	reduceinputs := make(map[int][]string)
	for i, _ := range j.Inputs {
		resp, err = cl.Get(eprefix+"map/"+strconv.Itoa(i)+"/"+"status", false, false)
		if err != nil {
			log.Fatal("Map tasks not yet allocated fully.. shouldnt get to here usually")
		}
		status, err := strconv.Atoi(resp.Node.Value)
		if err != nil {
			log.Fatal(err)
		}
		if status != StatusDone {
			log.Fatal("Map tasks not yet finished")
		}
		//Populate reduceinputs while we are at it...
		resp, err = cl.Get(eprefix+"map/"+strconv.Itoa(i)+"/"+"outputs", false, true)
		if err != nil {
			log.Fatal(err)
		}
		for _, node := range resp.Node.Nodes {
			splitted := strings.Split(node.Key, "/")
			partitionid, err := strconv.Atoi(splitted[len(splitted)-1])
			if err != nil {
				log.Fatal(err)
			}
			reduceinputs[partitionid] = append(reduceinputs[partitionid], node.Value)
			//log.Println(idx, node.Value)
		}
	}
	log.Println("Map phase completed, now onto Reduce...")

	//Update NumReduces
	_, err = cl.Update(eprefix+"numreduces", strconv.Itoa(len(reduceinputs)), 0)
	if err != nil {
		log.Fatal(err)
	}

	for i, inputs := range reduceinputs {
		_, err = cl.CreateDir(eprefix+"reduce/"+strconv.Itoa(i)+"/", 0)
		if err == nil {
			//Meaning we aquired lock for this phase...
			log.Println("Aquired lock for reduce task", i)
			//Update Status - we are obviously in map phase
			_, err = cl.Update(eprefix+"status", strconv.Itoa(StatusReduceStage), 0)
			if err != nil {
				log.Fatal(err)
			}

			//Update Status
			_, err = cl.Create(eprefix+"reduce/"+strconv.Itoa(i)+"/"+"status", strconv.Itoa(StatusInitialized), 0)
			if err != nil {
				log.Fatal(err)
			}

			//Start reduce task
			log.Println("Starting reduce task", i)
			output, err := w.Reduce(inputs, i, j)
			log.Println(output, err)
			if err != nil {
				log.Fatal(err)
			}
			//Write result to etcd
			_, err = cl.Create(eprefix+"results/"+strconv.Itoa(i), output, 0)
			if err != nil {
				log.Fatal(err)
			}
			//Update Status as done
			_, err = cl.Update(eprefix+"reduce/"+strconv.Itoa(i)+"/"+"status", strconv.Itoa(StatusDone), 0)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	//Check if all reduces have finished....
	for i, _ := range reduceinputs {
		resp, err = cl.Get(eprefix+"reduce/"+strconv.Itoa(i)+"/"+"status", false, false)
		if err != nil {
			log.Fatal("Reduce tasks not yet allocated fully.. shouldnt get to here usually")
		}
		status, err := strconv.Atoi(resp.Node.Value)
		if err != nil {
			log.Fatal(err)
		}
		if status != StatusDone {
			log.Fatal("Reduce tasks not yet finished")
		}
	}
	//Got to here means everything is done....
	log.Println("All tasks are done...")
	//Update status... doesnt matter if multiple workers invoke this...
	_, err = cl.Update(eprefix+"status", strconv.Itoa(StatusDone), 0)
	if err != nil {
		log.Fatal(err)
	}
}

//Stores S3 credentials and etcd locations
type Environment struct {
	AWS_ACCESS_KEY_ID     string   // AWS_ACCESS_KEY_ID
	AWS_SECRET_ACCESS_KEY string   //AWS_SECRET_ACCESS_KEY
	ETCD_SERVERS          []string //comma seperated contents of ETCD_SERVERS
	AWS_REGION            string   //AWS region as detected by goamz ( https://godoc.org/github.com/mitchellh/goamz/aws#pkg-variables )
	S3_BUCKET             string   //The default bucketname for new jobs
}

//Creates Environment data from reading environment variables
func NewEnvironment() *Environment {
	env := &Environment{
		AWS_ACCESS_KEY_ID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		AWS_SECRET_ACCESS_KEY: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		AWS_REGION:            os.Getenv("AWS_REGION"),
		S3_BUCKET:             os.Getenv("S3_BUCKET"),
	}
	for _, server := range strings.Split(os.Getenv("ETCD_SERVERS"), ",") {
		env.ETCD_SERVERS = append(env.ETCD_SERVERS, server)
	}
	return env
}

//Returns AWS auth from environment
func (env *Environment) GetAWSAuth() (aws.Auth, error) {
	return aws.GetAuth(env.AWS_ACCESS_KEY_ID, env.AWS_SECRET_ACCESS_KEY)
}

//Returns AWS region from environment
func (env *Environment) GetAWSRegion() (aws.Region, error) {
	region, ok := aws.Regions[env.AWS_REGION]
	if !ok {
		return region, errors.New("Region '" + env.AWS_REGION + "' not found")
	}
	return region, nil
}

//Returns S3 client from environment
func (env *Environment) GetS3() (*s3.S3, error) {
	auth, err := env.GetAWSAuth()
	if err != nil {
		return nil, err
	}
	region, err := env.GetAWSRegion()
	if err != nil {
		return nil, err
	}
	return s3.New(auth, region), nil
}

//Returns S3 bucket from environment
func (env *Environment) GetS3Bucket(bucketname string) (*s3.Bucket, error) {
	s, err := env.GetS3()
	if err != nil {
		return nil, err
	}
	return s.Bucket(bucketname), nil
}

//Returns etcd client
func (env *Environment) GetEtcdClient() *etcd.Client {
	return etcd.NewClient(env.ETCD_SERVERS)
}
