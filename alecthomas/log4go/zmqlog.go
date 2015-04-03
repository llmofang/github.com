package log4go

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	zmq "github.com/pebbe/zmq4"
)

type zmqLogRecord struct {
	Target  string
	Level   string    // The log level
	Created time.Time // The time at which the log message was created (nanoseconds)
	Source  string    // The message source
	Message string    // The log message
}

// This log writer sends output to a socket
type ZmqLogWriter chan *LogRecord

// This is the SocketLogWriter's output method
func (w ZmqLogWriter) LogWrite(rec *LogRecord) {
	w <- rec
}

func (w ZmqLogWriter) Close() {
	close(w)
}

func NewZmqLogWriter(hostport, target, topic string) ZmqLogWriter {
	sock, err := zmq.NewSocket(zmq.PUB)
	//	sock, err := net.Dial(proto, hostport)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewZmqLogWriter(%q): %s\n", hostport, err)
		return nil
	}

	if serr := sock.SetLinger(0); serr != nil {
		fmt.Fprintf(os.Stderr, "NewZmqLogWriter(%q): %s\n", hostport, serr)
		return nil
	}

	if serr := sock.SetSndhwm(100); serr != nil {
		fmt.Fprintf(os.Stderr, "NewZmqLogWriter(%q): %s\n", hostport, serr)
		return nil
	}

	//	if serr := sock.SetConflate(true); serr != nil {
	//		fmt.Fprintf(os.Stderr, "NewZmqLogWriter(%q): %s\n", hostport, serr)
	//		return nil
	//	}

	if cerr := sock.Connect(hostport); cerr != nil {
		fmt.Fprintf(os.Stderr, "NewZmqLogWriter(%q): %s\n", hostport, cerr)
		return nil
	}

	w := ZmqLogWriter(make(chan *LogRecord, LogBufferLength))

	go func() {
		defer func() {
			if sock != nil {
				sock.Disconnect(hostport)
				sock.Close()
			}
		}()

		lr := &zmqLogRecord{Target: target}

		for rec := range w {
			// Marshall into JSON
			lr.Level = levelStrings[rec.Level]
			lr.Created = rec.Created
			lr.Source = rec.Source
			lr.Message = rec.Message
			js, err := json.Marshal(lr)
			if err != nil {
				fmt.Fprint(os.Stderr, "ZmqLogWriter(%q): %s", hostport, err)
				continue
			}
			if _, err = sock.SendBytes(([]byte)(topic), zmq.SNDMORE); err != nil {
				fmt.Fprint(os.Stderr, "ZmqLogWriter(%q): %s", hostport, err)
			}

			if _, err = sock.SendBytes(js, zmq.DONTWAIT); err != nil {
				fmt.Fprint(os.Stderr, "ZmqLogWriter(%q): %s", hostport, err)
			}
		}
	}()

	return w
}
