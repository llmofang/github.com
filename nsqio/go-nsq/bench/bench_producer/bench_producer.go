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
	runfor     = flag.Duration("runfor", 10*time.Second, "duration of time to run")
	tcpAddress = flag.String("nsqd-tcp-address", "10.15.107.132:4150", "<addr>:<port> to connect to nsqd")
	topic      = flag.String("topic", "bench.M100000.dzhyun#ephemeral", "topic to receive messages on")
	size       = flag.Int("size", 32, "size of messages")
	batchSize  = flag.Int("batch-size", 1, "batch size of messages")
	deadline   = flag.String("deadline", "", "deadline to start the benchmark run")
	cpu        = flag.Int("cpu", 1, "cup number to use")
	userAgent  = flag.String("user-agent", "bench_consumer;disable-fin", "user agent")
)

var totalMsgCount int64

func main() {
	flag.Parse()
	var wg sync.WaitGroup

	log.SetPrefix("[bench_producer] ")

	msg := make([]byte, *size)
	batch := make([][]byte, *batchSize)
	for i := range batch {
		batch[i] = msg
	}

	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < *cpu; j++ {
		wg.Add(1)
		go func() {
			pubWorker(*runfor, *tcpAddress, *batchSize, batch, *topic, rdyChan, goChan)
			wg.Done()
		}()
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

func pubWorker(td time.Duration, tcpAddr string, batchSize int, batch [][]byte, topic string, rdyChan chan int, goChan chan int) {
	cfg := nsq.NewConfig()
	cfg.DialTimeout = 3 * time.Second
	cfg.WriteTimeout = 10 * time.Second
	cfg.UserAgent = *userAgent
	p, err := nsq.NewProducer(tcpAddr, cfg)
	if err != nil {
		panic(err.Error())
	}
	rdyChan <- 1
	<-goChan
	var msgCount int64
	endTime := time.Now().Add(td)
	for {
		var err error
		if batchSize == 1 {
			err = p.PublishAsync(topic, batch[0], nil)
		} else {
			err = p.MultiPublishAsync(topic, batch, nil)
		}
		if err != nil {
			panic(err.Error())
		}
		msgCount += int64(batchSize)
		if time.Now().After(endTime) {
			break
		}
	}
	atomic.AddInt64(&totalMsgCount, msgCount)
	p.Stop()
}
