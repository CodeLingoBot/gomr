package gomr

import (
	"fmt"
	"github.com/segmentio/go-loggly"
	"log"
	"os"
	"time"
)

type LogLine struct {
	TimeStamp time.Time
	Level     string
	Text      string
}

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
	client *loggly.Client
}

func NewLogglyLog(token string, tags []string) *LogglyLog {
	return &LogglyLog{loggly.New(token, tags...)}
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

func (c *LogglyLog) Fetch(tag string, n int) []LogLine {
	//TODO
	return []LogLine{}
}

//Send to loggly as well as console
type LogglyConsoleLog struct {
	logglycl  *LogglyLog
	consolecl *ConsoleLog
}

func NewLogglyConsoleLog(token string, tags []string) *LogglyConsoleLog {
	return &LogglyConsoleLog{NewLogglyLog(token, tags), NewConsoleLog(tags)}
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
	//TODO
	return c.logglycl.Fetch(tag, n)
}
