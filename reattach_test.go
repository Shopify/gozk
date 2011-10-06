package zk_test

import (
	"bufio"
	. "launchpad.net/gocheck"
	"launchpad.net/gozk/zk"
	"exec"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

var reattach = flag.Bool("zktest.reattach", false, "internal flag used for testing")
var reattachRunDir = flag.String("zktest.rundir", "", "internal flag used for testing")
var reattachAbnormalStop = flag.Bool("zktest.stop", false, "internal flag used for testing")

// This is the reentrancy point for testing ZooKeeper servers
// started by processes that are not direct children of the
// testing process. This test always succeeds - the status
// will be written to stdout and read by indirectServer.
func TestStartNonChildServer(t *testing.T) {
	if !*reattach {
		// not re-entrant, so ignore this test.
		return
	}
	err := startServer(*reattachRunDir, *reattachAbnormalStop)
	if err != nil {
		fmt.Printf("error:%v\n", err)
		return
	}
	fmt.Printf("done\n")
}

func (s *S) startServer(c *C, abort bool) {
	err := startServer(s.zkTestRoot, abort)
	c.Assert(err, IsNil)
}

// startServerIndirect starts a ZooKeeper server that is not
// a direct child of the current process. If abort is true,
// the server will be started and then terminated abnormally.
func (s *S) startServerIndirect(c *C, abort bool) {
	if len(os.Args) == 0 {
		c.Fatal("Cannot find self executable name")
	}
	cmd := exec.Command(
		os.Args[0],
		"-zktest.reattach",
		"-zktest.rundir", s.zkTestRoot,
		"-zktest.stop=", fmt.Sprint(abort),
		"-test.run", "StartNonChildServer",
	)
	r, err := cmd.StdoutPipe()
	c.Assert(err, IsNil)
	defer r.Close()
	if err := cmd.Start(); err != nil {
		c.Fatalf("cannot start re-entrant gotest process: %v", err)
	}
	defer cmd.Wait()
	bio := bufio.NewReader(r)
	for {
		line, err := bio.ReadSlice('\n')
		if err != nil {
			c.Fatalf("indirect server status line not found: %v", err)
		}
		s := string(line)
		if strings.HasPrefix(s, "error:") {
			c.Fatalf("indirect server error: %s", s[len("error:"):])
		}
		if s == "done\n" {
			return
		}
	}
	panic("not reached")
}

// startServer starts a ZooKeeper server, and terminates it abnormally
// if abort is true.
func startServer(runDir string, abort bool) os.Error {
	srv, err := zk.AttachServer(runDir)
	if err != nil {
		return fmt.Errorf("cannot attach to server at %q: %v", runDir, err)
	}
	if err := srv.Start(); err != nil {
		return fmt.Errorf("cannot start server: %v", err)
	}
	if abort {
		// Give it time to start up, then kill the server process abnormally,
		// leaving the pid.txt file behind.
		time.Sleep(0.5e9)
		p, err := srv.Process()
		if err != nil {
			return fmt.Errorf("cannot get server process: %v", err)
		}
		defer p.Release()
		if err := p.Kill(); err != nil {
			return fmt.Errorf("cannot kill server process: %v", err)
		}
	}
	return nil
}

func (s *S) checkCookie(c *C) {
	conn, watch, err := zk.Dial(s.zkAddr, 5e9)
	c.Assert(err, IsNil)

	e, ok := <-watch
	c.Assert(ok, Equals, true)
	c.Assert(e.Ok(), Equals, true)

	c.Assert(err, IsNil)
	cookie, _, err := conn.Get("/testAttachCookie")
	c.Assert(err, IsNil)
	c.Assert(cookie, Equals, "testAttachCookie")
	conn.Close()
}

// cases to test:
// child server, stopped normally; reattach, start
// non-direct child server, killed abnormally; reattach, start (->error), remove pid.txt; start
// non-direct child server, still running; reattach, start (->error), stop, start
// child server, still running; reattach, start (-> error)
// child server, still running; reattach, stop, start.
// non-direct child server, still running; reattach, stop, start.
func (s *S) TestAttachServer(c *C) {
	// Create a cookie so that we know we are reattaching to the same instance.
	conn, _ := s.init(c)
	_, err := conn.Create("/testAttachCookie", "testAttachCookie", 0, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)
	s.checkCookie(c)
	s.zkServer.Stop()
	s.zkServer = nil

	s.testAttachServer(c, (*S).startServer)
	s.testAttachServer(c, (*S).startServerIndirect)
	s.testAttachServerAbnormalTerminate(c, (*S).startServer)
	s.testAttachServerAbnormalTerminate(c, (*S).startServerIndirect)

	srv, err := zk.AttachServer(s.zkTestRoot)
	c.Assert(err, IsNil)

	s.zkServer = srv
	err = s.zkServer.Start()
	c.Assert(err, IsNil)

	conn, _ = s.init(c)
	err = conn.Delete("/testAttachCookie", -1)
	c.Assert(err, IsNil)
}

func (s *S) testAttachServer(c *C, start func(*S, *C, bool)) {
	start(s, c, false)

	s.checkCookie(c)

	// try attaching to it while it is still running - it should fail.
	srv, err := zk.AttachServer(s.zkTestRoot)
	c.Assert(err, IsNil)

	err = srv.Start()
	c.Assert(err, NotNil)

	// stop it and then start it again - it should succeed.
	err = srv.Stop()
	c.Assert(err, IsNil)

	err = srv.Start()
	c.Assert(err, IsNil)

	s.checkCookie(c)

	err = srv.Stop()
	c.Assert(err, IsNil)
}

func (s *S) testAttachServerAbnormalTerminate(c *C, start func(*S, *C, bool)) {
	start(s, c, true)

	// try attaching to it and starting - it should fail, because pid.txt
	// won't have been removed.
	srv, err := zk.AttachServer(s.zkTestRoot)
	c.Assert(err, IsNil)
	err = srv.Start()
	c.Assert(err, NotNil)

	// stopping it should bring things back to normal.
	err = srv.Stop()
	c.Assert(err, IsNil)
	err = srv.Start()
	c.Assert(err, IsNil)

	s.checkCookie(c)
	err = srv.Stop()
	c.Assert(err, IsNil)
}
