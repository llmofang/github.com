package main

import (
	"flag"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
)

var (
	runfor      = flag.Duration("runfor", 10*time.Second, "duration of time to run")
	httpAddress = flag.String("lookupd-http-address", "10.15.107.132:4161", "<addr>:<port> to connect to lookupd")
	size        = flag.Int("size", 1, "size of messages")
	topic       = flag.String("topic", "bench.M100000.dzhyun#ephemeral", "topic to receive messages on")
	channel     = flag.String("channel", "c1.M10000#ephemeral", "channel to receive messages on")
	deadline    = flag.String("deadline", "", "deadline to start the benchmark run")
	rdy         = flag.Int("rdy", 2500, "RDY count to use")
	cpu         = flag.Int("cpu", 1, "cup number to use")
)

var totalMsgCount int64

type handler struct {
	msgCount int64
}

func (self *handler) HandleMessage(message *nsq.Message) error {
	self.msgCount++
	return nil
}

func main() {
	flag.Parse()
	var wg sync.WaitGroup

	log.SetPrefix("[bench_consumer] ")

	goChan := make(chan int)
	rdyChan := make(chan int)
	workers := *cpu
	for j := 0; j < workers; j++ {
		wg.Add(1)
		go func(id int) {
			subWorker(*runfor, workers, *httpAddress, *topic, *channel, rdyChan, goChan, id)
			wg.Done()
		}(j)
		<-rdyChan
	}

	if *deadline != "" {
		t, err := time.Parse("2006-01-02 15:04:05", *deadline)
		if err != nil {
			log.Fatal(err)
		}
		d := t.Sub(time.Now())
		log.Printf("sleeping until %s (%s)", t, d)
		time.Sleep(d)
	}

	start := time.Now()
	close(goChan)
	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)
	tmc := atomic.LoadInt64(&totalMsgCount)
	log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op",
		duration,
		float64(tmc*int64(*size))/duration.Seconds()/1024/1024,
		float64(tmc)/duration.Seconds(),
		float64(duration/time.Microsecond)/float64(tmc))
}

func subWorker(td time.Duration, workers int, httpAddr string, topic string, channel string, rdyChan chan int, goChan chan int, id int) {
	cfg := nsq.NewConfig()
	cfg.DialTimeout = 3 * time.Second
	cfg.UserAgent = "bench_consumer;disable-fin"
	cfg.MaxInFlight = 2500
	c, err := nsq.NewConsumer(topic, channel, cfg)
	if err != nil {
		panic(err.Error())
	}
	h := &handler{}
	c.AddHandler(h)
	err = c.ConnectToNSQLookupd(httpAddr)
	if err != nil {
		panic(err.Error())
	}
	rdyChan <- 1
	<-goChan
	time.Sleep(td)
	c.Stop()

	atomic.AddInt64(&totalMsgCount, h.msgCount)
}
