package gomr

import (
	"encoding/json"
	"fmt"
	"github.com/segmentio/go-loggly"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

type LogLine struct {
	TimeStamp time.Time
	Hostname  string
	Level     string
	Text      string
}

//Logger interface defines logging capability available to workers.
//
//To start with we have the capability to use loggly and/or console log.
type Logger interface {
	Info(v ...interface{})
	Critical(v ...interface{})
	Close()
	Fetch(tag string, n int) []LogLine //Retrieve last n lines of logs
}

//Print to stderr
type ConsoleLog struct {
	tags []string
}

func NewConsoleLog(tags []string) *ConsoleLog {
	hostname, err := os.Hostname()
	if err == nil {
		tags = append(tags, hostname)
	}
	return &ConsoleLog{tags}
}

func (c *ConsoleLog) Info(v ...interface{}) {
	log.Println(c.tags, v)
}

func (c *ConsoleLog) Critical(v ...interface{}) {
	log.Println(c.tags, v)
}

func (c *ConsoleLog) Close() {
	//Do nothing. This is here to satisfy the interface
}

func (c *ConsoleLog) Fetch(tag string, n int) []LogLine {
	//Do nothing. This is here to satisfy the interface
	return []LogLine{}
}

//Loggly
type LogglyLog struct {
	client                      *loggly.Client
	account, username, password string
}

func NewLogglyLog(token string, tags []string, account, username, password string) *LogglyLog {
	return &LogglyLog{loggly.New(token, tags...), account, username, password}
}

func (c *LogglyLog) Info(v ...interface{}) {
	c.client.Info(fmt.Sprintln(v...))
}

func (c *LogglyLog) Critical(v ...interface{}) {
	c.client.Critical(fmt.Sprintln(v...))
}

func (c *LogglyLog) Close() {
	c.client.Flush() //We are probably going out of scopt... flush it now
}

type logglyresp struct {
	Rsid struct {
		Id string
	}
}

type logglyevent struct {
	Event struct {
		Json struct {
			Timestamp int64
			Hostname  string
			Type      string
			Level     string
		}
	}
}

type logglydata struct {
	Events []logglyevent
}

func (c *LogglyLog) Fetch(tag string, n int) []LogLine {
	result := []LogLine{}
	url := fmt.Sprintf("https://%s.loggly.com/apiv2/search?q=tag:\"%s\"&until=now&size=%d", c.account, tag, n)
	log.Println("Fetch", url)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Println(err)
		return result
	}
	req.SetBasicAuth(c.username, c.password)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return result
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Println(resp.StatusCode)
		return result
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return result
	}
	lresp := &logglyresp{}
	err = json.Unmarshal(data, lresp)
	if err != nil {
		log.Println(err)
		return result
	}

	rsid := lresp.Rsid.Id
	log.Println(rsid)
	url = fmt.Sprintf("https://%s.loggly.com/apiv2/events?rsid=%s", c.account, rsid)
	log.Println("Fetch", url)
	req, err = http.NewRequest("GET", url, nil)
	if err != nil {
		log.Println(err)
		return result
	}
	req.SetBasicAuth(c.username, c.password)
	resp, err = client.Do(req)
	if err != nil {
		log.Println(err)
		return result
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Println(resp.StatusCode)
		return result
	}
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return result
	}
	items := &logglydata{}
	err = json.Unmarshal(data, items)
	if err != nil {
		log.Println(err)
		return result
	}
	for _, item := range items.Events {
		//log.Println(item)
		seconds := item.Event.Json.Timestamp / 1000
		ll := LogLine{
			TimeStamp: time.Unix(seconds, 0),
			Hostname:  item.Event.Json.Hostname,
			Level:     item.Event.Json.Level,
			Text:      item.Event.Json.Type,
		}
		result = append(result, ll)
	}
	return result
}

//Send to loggly as well as console
type LogglyConsoleLog struct {
	logglycl  *LogglyLog
	consolecl *ConsoleLog
}

func NewLogglyConsoleLog(token string, tags []string, account, username, password string) *LogglyConsoleLog {
	return &LogglyConsoleLog{NewLogglyLog(token, tags, account, username, password), NewConsoleLog(tags)}
}

func (c *LogglyConsoleLog) Info(v ...interface{}) {
	c.logglycl.Info(v)
	c.consolecl.Info(v)
}

func (c *LogglyConsoleLog) Critical(v ...interface{}) {
	c.logglycl.Critical(v)
	c.consolecl.Critical(v)
}

func (c *LogglyConsoleLog) Close() {
	c.logglycl.Close()
	c.consolecl.Close()
}

func (c *LogglyConsoleLog) Fetch(tag string, n int) []LogLine {
	//Only fetch from loggly because console logging has no fetch capability
	return c.logglycl.Fetch(tag, n)
}
