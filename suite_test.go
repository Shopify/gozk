package zk_test

import (
	. "launchpad.net/gocheck"
	"bufio"
	"exec"
	"fmt"
	zk "launchpad.net/gozk/zookeeper"
	"os"
	"testing"
	"time"
)

func TestAll(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&S{})

type S struct {
	zkArgs     []string
	zkTestRoot string
	zkTestPort int
	zkProcess  *os.Process // The running ZooKeeper process
	zkAddr     string

	handles     []*zk.Conn
	events      []*zk.Event
	liveWatches int
	deadWatches chan bool
}

var logLevel = 0 //zk.LOG_ERROR

func (s *S) init(c *C) (*zk.Conn, chan zk.Event) {
	conn, watch, err := zk.Dial(s.zkAddr, 5e9)
	c.Assert(err, IsNil)

	s.handles = append(s.handles, conn)

	event := <-watch

	c.Assert(event.Type, Equals, zk.EVENT_SESSION)
	c.Assert(event.State, Equals, zk.STATE_CONNECTED)

	bufferedWatch := make(chan zk.Event, 256)
	bufferedWatch <- event

	s.liveWatches += 1
	go func() {
	loop:
		for {
			select {
			case event, ok := <-watch:
				if !ok {
					close(bufferedWatch)
					break loop
				}
				select {
				case bufferedWatch <- event:
				default:
					panic("Too many events in buffered watch!")
				}
			}
		}
		s.deadWatches <- true
	}()

	return conn, bufferedWatch
}

func (s *S) SetUpTest(c *C) {
	c.Assert(zk.CountPendingWatches(), Equals, 0,
		Bug("Test got a dirty watch state before running!"))
	zk.SetLogLevel(logLevel)
}

func (s *S) TearDownTest(c *C) {
	// Close all handles opened in s.init().
	for _, handle := range s.handles {
		handle.Close()
	}

	// Wait for all the goroutines created in s.init() to terminate.
	for s.liveWatches > 0 {
		select {
		case <-s.deadWatches:
			s.liveWatches -= 1
		case <-time.After(5e9):
			panic("There's a locked watch goroutine :-(")
		}
	}

	// Reset the list of handles.
	s.handles = make([]*zk.Conn, 0)

	c.Assert(zk.CountPendingWatches(), Equals, 0,
		Bug("Test left live watches behind!"))
}

// We use the suite set up and tear down to manage a custom ZooKeeper
//
func (s *S) SetUpSuite(c *C) {
	var err os.Error
	s.deadWatches = make(chan bool)

	s.zkTestRoot = c.MkDir()
	s.zkTestPort = 21812
	s.zkAddr = fmt.Sprint("localhost:", s.zkTestPort)

	s.zkArgs, err = zk.Server(s.zkTestPort, s.zkTestRoot, "")
	if err != nil {
		c.Fatal("Cannot set up server environment: ", err)
	}
	s.StartZK(c)
}

func (s *S) TearDownSuite(c *C) {
	s.StopZK()
}

func startLogger(c *C, cmd *exec.Cmd) {
	r, err := cmd.StdoutPipe()
	if err != nil {
		c.Fatal("cannot make output pipe:", err)
	}
	cmd.Stderr = cmd.Stdout
	bio := bufio.NewReader(r)
	go func() {
		for {
			line, err := bio.ReadSlice('\n')
			if err != nil {
				break
			}
			c.Log(line[0 : len(line)-1])
		}
	}()
}

func (s *S) StartZK(c *C) {
	cmd := exec.Command(s.zkArgs[0], s.zkArgs[1:]...)
	startLogger(c, cmd)
	err := cmd.Start()
	if err != nil {
		c.Fatal("Error starting zookeeper server: ", err)
	}
	s.zkProcess = cmd.Process
}

func (s *S) StopZK() {
	if s.zkProcess != nil {
		s.zkProcess.Kill()
		s.zkProcess.Wait(0)
	}
	s.zkProcess = nil
}
