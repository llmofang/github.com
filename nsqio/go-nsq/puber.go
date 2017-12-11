package nsq

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Producer is a high-level type to publish to NSQ.

var (
	ErrNoValidProducer = errors.New("no valid producer")
)

type Puber struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	mtx sync.RWMutex

	logger   logger
	logLvl   LogLevel
	logGuard sync.RWMutex

	behaviorDelegate interface{}

	id     int64
	config Config

	rngMtx sync.Mutex
	rng    *rand.Rand

	pendingConnections map[string]*Producer
	connections        []*Producer

	nextConnIndex int

	nsqdTCPAddrs []string

	// used at connection close to force a possible reconnect
	lookupdRecheckChan chan int
	lookupdHTTPAddrs   []string
	lookupdQueryIndex  int

	wg            sync.WaitGroup
	stopFlag      int32
	connectedFlag int32
	stopHandler   sync.Once
	exitHandler   sync.Once

	// read from this channel to block until consumer is cleanly stopped
	StopChan chan int
	exitChan chan int
}

// NewProducer returns an instance of Producer for the specified address
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewProducer the values are no longer mutable (they are copied).
func NewPuber(config *Config) (*Puber, error) {
	config.assertInitialized()
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	r := &Puber{
		id: atomic.AddInt64(&instCount, 1),

		config: *config,

		logger: log.New(os.Stderr, "", log.Flags()),
		logLvl: LogLevelInfo,

		pendingConnections: make(map[string]*Producer),
		connections:        make([]*Producer, 0),

		lookupdRecheckChan: make(chan int, 1),

		StopChan: make(chan int),
		exitChan: make(chan int),
	}

	return r, nil
}

// ConnectToNSQLookupd adds an nsqlookupd address to the list for this Consumer instance.
//
// If it is the first to be added, it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (r *Puber) ConnectToNSQLookupd(addr string) error {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}

	if err := validatedLookupAddr(addr); err != nil {
		return err
	}

	atomic.StoreInt32(&r.connectedFlag, 1)

	r.mtx.Lock()
	for _, x := range r.lookupdHTTPAddrs {
		if x == addr {
			r.mtx.Unlock()
			return nil
		}
	}
	r.lookupdHTTPAddrs = append(r.lookupdHTTPAddrs, addr)
	numLookupd := len(r.lookupdHTTPAddrs)
	r.mtx.Unlock()

	// if this is the first one, kick off the go loop
	if numLookupd == 1 {
		r.queryLookupd()
		r.wg.Add(1)
		go r.lookupdLoop()
	}

	return nil
}

// ConnectToNSQLookupds adds multiple nsqlookupd address to the list for this Consumer instance.
//
// If adding the first address it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (r *Puber) ConnectToNSQLookupds(addresses []string) error {
	for _, addr := range addresses {
		err := r.ConnectToNSQLookupd(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

// return the next lookupd endpoint to query
// keeping track of which one was last used
func (r *Puber) nextLookupdEndpoint() string {
	r.mtx.RLock()
	if r.lookupdQueryIndex >= len(r.lookupdHTTPAddrs) {
		r.lookupdQueryIndex = 0
	}
	addr := r.lookupdHTTPAddrs[r.lookupdQueryIndex]
	num := len(r.lookupdHTTPAddrs)
	r.mtx.RUnlock()
	r.lookupdQueryIndex = (r.lookupdQueryIndex + 1) % num

	urlString := addr
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + addr
	}

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	if u.Path == "/" || u.Path == "" {
		u.Path = "/nodes"
	}

	v, err := url.ParseQuery(u.RawQuery)
	u.RawQuery = v.Encode()
	return u.String()
}

type nodesResp struct {
	Producers []*peerInfo `json:"producers"`
}

// make an HTTP req to one of the configured nsqlookupd instances to discover
// which nsqd's provide the topic we are consuming.
//
// initiate a connection to any new producers that are identified.
func (r *Puber) queryLookupd() {
	retries := 0

retry:
	endpoint := r.nextLookupdEndpoint()

	r.log(LogLevelInfo, "querying nsqlookupd %s", endpoint)

	var data nodesResp
	err := apiRequestNegotiateV1("GET", endpoint, nil, &data)
	if err != nil {
		r.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
		retries++
		if retries < 3 {
			r.log(LogLevelInfo, "retrying with next nsqlookupd")
			goto retry
		}
		return
	}

	var nsqdAddrs []string
	for _, producer := range data.Producers {
		broadcastAddress := producer.BroadcastAddress
		port := producer.TCPPort
		joined := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		nsqdAddrs = append(nsqdAddrs, joined)
	}
	// apply filter
	if discoveryFilter, ok := r.behaviorDelegate.(DiscoveryFilter); ok {
		nsqdAddrs = discoveryFilter.Filter(nsqdAddrs)
	}
	for _, addr := range nsqdAddrs {
		err = r.connectToNSQD(addr)
		if err != nil && err != ErrAlreadyConnected {
			r.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
			continue
		}
	}
}

// poll all known lookup servers every LookupdPollInterval
func (r *Puber) lookupdLoop() {
	// add some jitter so that multiple consumers discovering the same topic,
	// when restarted at the same time, dont all connect at once.
	r.rngMtx.Lock()
	jitter := time.Duration(int64(r.rng.Float64() *
		r.config.LookupdPollJitter * float64(r.config.LookupdPollInterval)))
	r.rngMtx.Unlock()
	var ticker *time.Ticker

	select {
	case <-time.After(jitter):
	case <-r.exitChan:
		goto exit
	}

	ticker = time.NewTicker(r.config.LookupdPollInterval)

	for {
		select {
		case <-ticker.C:
			r.queryLookupd()
		case <-r.lookupdRecheckChan:
			r.queryLookupd()
		case <-r.exitChan:
			goto exit
		}
	}

exit:
	if ticker != nil {
		ticker.Stop()
	}
	r.log(LogLevelInfo, "exiting lookupdLoop")
	r.wg.Done()
}

// connectToNSQD takes a nsqd address to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to a single, local,
// instance.
func (r *Puber) connectToNSQD(addr string) error {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}

	atomic.StoreInt32(&r.connectedFlag, 1)

	logger, logLvl := r.getLogger()

	conn, _ := NewProducer(addr, &r.config)
	conn.SetDelegate(&puberProducerDelegate{r})
	conn.SetLogger(logger, logLvl)

	r.mtx.Lock()
	_, pendingOk := r.pendingConnections[addr]
	_, pos := r.findConn(addr)
	if pos != -1 || pendingOk {
		r.mtx.Unlock()
		return ErrAlreadyConnected
	}
	r.pendingConnections[addr] = conn
	if idx := indexOf(addr, r.nsqdTCPAddrs); idx == -1 {
		r.nsqdTCPAddrs = append(r.nsqdTCPAddrs, addr)
	}
	r.mtx.Unlock()

	r.log(LogLevelInfo, "(%s) connecting to nsqd", addr)

	cleanupConnection := func() {
		r.mtx.Lock()
		delete(r.pendingConnections, addr)
		r.mtx.Unlock()
		conn.Stop()
	}

	err := conn.Ping()
	if err != nil {
		cleanupConnection()
		return err
	}

	r.mtx.Lock()
	delete(r.pendingConnections, addr)
	r.connections = append(r.connections, conn)
	r.mtx.Unlock()

	return nil
}

func (r *Puber) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := r.getLogger()

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %3d %s",
		lvl, r.id, fmt.Sprintf(line, args...)))
}

func (r *Puber) getLogger() (logger, LogLevel) {
	r.logGuard.RLock()
	defer r.logGuard.RUnlock()

	return r.logger, r.logLvl
}

func (r *Puber) conns() []*Producer {
	r.mtx.RLock()
	conns := make([]*Producer, len(r.connections))
	copy(conns, r.connections)
	r.mtx.RUnlock()
	return conns
}

func (r *Puber) nextConn() (*Producer, error) {
	r.mtx.Lock()
	if len(r.connections) == 0 {
		return nil, ErrNoValidProducer
	}
	if r.nextConnIndex >= len(r.connections) {
		r.nextConnIndex = 0
	}
	conn := r.connections[r.nextConnIndex]
	r.mtx.Unlock()
	return conn, nil
}

func (r *Puber) findConn(addr string) (*Producer, int) {
	for i, c := range r.connections {
		if c.String() == addr {
			return c, i
		}
	}
	return nil, -1
}

func (r *Puber) indexConn(conn *Producer) int {
	for i, c := range r.connections {
		if c == conn {
			return i
		}
	}
	return -1
}

func (r *Puber) removeConn(conn *Producer) {
	pos := r.indexConn(conn)
	if pos == -1 {
		return
	}
	conns := make([]*Producer, len(r.connections)-1)
	copy(conns, r.connections[:pos])
	copy(conns, r.connections[pos+1:])
	r.connections = conns
}

// PublishAsync publishes a message body to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (r *Puber) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	conn, err := r.nextConn()
	if err != nil {
		return err
	}
	return conn.PublishAsync(topic, body, doneChan, args)
}

// MultiPublishAsync publishes a slice of message bodies to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (r *Puber) MultiPublishAsync(topic string, body [][]byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	conn, err := r.nextConn()
	if err != nil {
		return err
	}
	return conn.MultiPublishAsync(topic, body, doneChan, args)
}

// Publish synchronously publishes a message body to the specified topic, returning
// an error if publish failed
func (r *Puber) Publish(topic string, body []byte) error {
	conn, err := r.nextConn()
	if err != nil {
		return err
	}
	return conn.Publish(topic, body)
}

// MultiPublish synchronously publishes a slice of message bodies to the specified topic, returning
// an error if publish failed
func (r *Puber) MultiPublish(topic string, body [][]byte) error {
	conn, err := r.nextConn()
	if err != nil {
		return err
	}
	return conn.MultiPublish(topic, body)
}

// DeferredPublish synchronously publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires, returning
// an error if publish failed
func (r *Puber) DeferredPublish(topic string, delay time.Duration, body []byte) error {
	conn, err := r.nextConn()
	if err != nil {
		return err
	}
	return conn.DeferredPublish(topic, delay, body)
}

// DeferredPublishAsync publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (r *Puber) DeferredPublishAsync(topic string, delay time.Duration, body []byte,
	doneChan chan *ProducerTransaction, args ...interface{}) error {
	conn, err := r.nextConn()
	if err != nil {
		return err
	}
	return conn.DeferredPublishAsync(topic, delay, body, doneChan, args)
}

func (r *Puber) onProducerClose(p *Producer) {
	r.mtx.Lock()
	r.removeConn(p)
	numLookupd := len(r.lookupdHTTPAddrs)
	r.mtx.Unlock()
	if numLookupd > 0 {
		// trigger a poll of the lookupd
		select {
		case r.lookupdRecheckChan <- 1:
		default:
		}
	}
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (r *Puber) Stop() {
	if !atomic.CompareAndSwapInt32(&r.stopFlag, 0, 1) {
		return
	}

	r.log(LogLevelInfo, "stopping...")

	for _, c := range r.conns() {
		c.Stop()
	}

	time.AfterFunc(time.Second*30, func() {
		// if we've waited this long handlers are blocked on processing messages
		// so we can't just stopHandlers (if any adtl. messages were pending processing
		// we would cause a panic on channel close)
		//
		// instead, we just bypass handler closing and skip to the final exit
		r.exit()
	})
}

func (r *Puber) exit() {
	r.exitHandler.Do(func() {
		close(r.exitChan)
		r.wg.Wait()
		close(r.StopChan)
	})
}
