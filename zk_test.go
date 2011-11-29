package zookeeper_test

import (
	. "launchpad.net/gocheck"
	zk "launchpad.net/gozk/zookeeper"
	"time"
)

// This error will be delivered via C errno, since ZK unfortunately
// only provides the handler back from zookeeper_init().
func (s *S) TestInitErrorThroughErrno(c *C) {
	conn, watch, err := zk.Dial("bad-domain-without-port", 5e9)
	if conn != nil {
		conn.Close()
	}
	if watch != nil {
		go func() {
			for {
				_, ok := <-watch
				if !ok {
					break
				}
			}
		}()
	}
	c.Assert(conn, IsNil)
	c.Assert(watch, IsNil)
	c.Assert(err, Matches, "invalid argument")
}

func (s *S) TestRecvTimeoutInitParameter(c *C) {
	conn, watch, err := zk.Dial(s.zkAddr, 0)
	c.Assert(err, IsNil)
	defer conn.Close()

	select {
	case <-watch:
		c.Fatal("Watch fired")
	default:
	}

	for i := 0; i != 1000; i++ {
		_, _, err := conn.Get("/zookeeper")
		if err != nil {
			c.Assert(err, Matches, "operation timeout")
			c.SucceedNow()
		}
	}

	c.Fatal("Operation didn't timeout")
}

func (s *S) TestSessionWatches(c *C) {
	c.Assert(zk.CountPendingWatches(), Equals, 0)

	zk1, watch1 := s.init(c)
	zk2, watch2 := s.init(c)
	zk3, watch3 := s.init(c)

	c.Assert(zk.CountPendingWatches(), Equals, 3)

	event1 := <-watch1
	c.Assert(event1.Type, Equals, zk.EVENT_SESSION)
	c.Assert(event1.State, Equals, zk.STATE_CONNECTED)

	c.Assert(zk.CountPendingWatches(), Equals, 3)

	event2 := <-watch2
	c.Assert(event2.Type, Equals, zk.EVENT_SESSION)
	c.Assert(event2.State, Equals, zk.STATE_CONNECTED)

	c.Assert(zk.CountPendingWatches(), Equals, 3)

	event3 := <-watch3
	c.Assert(event3.Type, Equals, zk.EVENT_SESSION)
	c.Assert(event3.State, Equals, zk.STATE_CONNECTED)

	c.Assert(zk.CountPendingWatches(), Equals, 3)

	zk1.Close()
	c.Assert(zk.CountPendingWatches(), Equals, 2)
	zk2.Close()
	c.Assert(zk.CountPendingWatches(), Equals, 1)
	zk3.Close()
	c.Assert(zk.CountPendingWatches(), Equals, 0)
}

// Gozk injects a STATE_CLOSED event when conn.Close() is called, right
// before the channel is closed.  Closing the channel injects a nil
// pointer, as usual for Go, so the STATE_CLOSED gives a chance to
// know that a nil pointer is coming, and to stop the procedure.
// Hopefully this procedure will avoid some nil-pointer references by
// mistake.
func (s *S) TestClosingStateInSessionWatch(c *C) {
	conn, watch := s.init(c)

	event := <-watch
	c.Assert(event.Type, Equals, zk.EVENT_SESSION)
	c.Assert(event.State, Equals, zk.STATE_CONNECTED)

	conn.Close()
	event, ok := <-watch
	c.Assert(ok, Equals, false)
	c.Assert(event.Type, Equals, zk.EVENT_CLOSED)
	c.Assert(event.State, Equals, zk.STATE_CLOSED)
}

func (s *S) TestEventString(c *C) {
	var event zk.Event
	event = zk.Event{zk.EVENT_SESSION, "/path", zk.STATE_CONNECTED}
	c.Assert(event, Matches, "ZooKeeper connected")
	event = zk.Event{zk.EVENT_CREATED, "/path", zk.STATE_CONNECTED}
	c.Assert(event, Matches, "ZooKeeper connected; path created: /path")
	event = zk.Event{-1, "/path", zk.STATE_CLOSED}
	c.Assert(event, Matches, "ZooKeeper connection closed")
}

var okTests = []struct {
	zk.Event
	Ok bool
}{
	{zk.Event{zk.EVENT_SESSION, "", zk.STATE_CONNECTED}, true},
	{zk.Event{zk.EVENT_CREATED, "", zk.STATE_CONNECTED}, true},
	{zk.Event{0, "", zk.STATE_CLOSED}, false},
	{zk.Event{0, "", zk.STATE_EXPIRED_SESSION}, false},
	{zk.Event{0, "", zk.STATE_AUTH_FAILED}, false},
}

func (s *S) TestEventOk(c *C) {
	for _, t := range okTests {
		c.Assert(t.Event.Ok(), Equals, t.Ok)
	}
}

func (s *S) TestGetAndStat(c *C) {
	conn, _ := s.init(c)

	data, stat, err := conn.Get("/zookeeper")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "")
	c.Assert(stat.Czxid(), Equals, int64(0))
	c.Assert(stat.Mzxid(), Equals, int64(0))
	c.Assert(stat.CTime(), Equals, int64(0))
	c.Assert(stat.MTime(), Equals, int64(0))
	c.Assert(stat.Version(), Equals, int32(0))
	c.Assert(stat.CVersion(), Equals, int32(0))
	c.Assert(stat.AVersion(), Equals, int32(0))
	c.Assert(stat.EphemeralOwner(), Equals, int64(0))
	c.Assert(stat.DataLength(), Equals, int32(0))
	c.Assert(stat.NumChildren(), Equals, int32(1))
	c.Assert(stat.Pzxid(), Equals, int64(0))
}

func (s *S) TestGetAndError(c *C) {
	conn, _ := s.init(c)

	data, stat, err := conn.Get("/non-existent")

	c.Assert(data, Equals, "")
	c.Assert(stat, IsNil)
	c.Assert(err, Matches, "no node")
	c.Assert(err, Equals, zk.ZNONODE)
}

func (s *S) TestCreateAndGet(c *C) {
	conn, _ := s.init(c)

	path, err := conn.Create("/test-", "bababum", zk.SEQUENCE|zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)
	c.Assert(path, Matches, "/test-[0-9]+")

	// Check the error condition from Create().
	_, err = conn.Create(path, "", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, Matches, "node exists")

	data, _, err := conn.Get(path)
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "bababum")
}

func (s *S) TestCreateSetAndGet(c *C) {
	conn, _ := s.init(c)

	_, err := conn.Create("/test", "", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	stat, err := conn.Set("/test", "bababum", -1) // Any version.
	c.Assert(err, IsNil)
	c.Assert(stat.Version(), Equals, int32(1))

	data, _, err := conn.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "bababum")
}

func (s *S) TestGetAndWatch(c *C) {
	c.Check(zk.CountPendingWatches(), Equals, 0)

	conn, _ := s.init(c)

	c.Check(zk.CountPendingWatches(), Equals, 1)

	_, err := conn.Create("/test", "one", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	data, stat, watch, err := conn.GetW("/test")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "one")
	c.Assert(stat.Version(), Equals, int32(0))

	select {
	case <-watch:
		c.Fatal("Watch fired")
	default:
	}

	c.Check(zk.CountPendingWatches(), Equals, 2)

	_, err = conn.Set("/test", "two", -1)
	c.Assert(err, IsNil)

	event := <-watch
	c.Assert(event.Type, Equals, zk.EVENT_CHANGED)

	c.Check(zk.CountPendingWatches(), Equals, 1)

	data, _, watch, err = conn.GetW("/test")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "two")

	select {
	case <-watch:
		c.Fatal("Watch fired")
	default:
	}

	c.Check(zk.CountPendingWatches(), Equals, 2)

	_, err = conn.Set("/test", "three", -1)
	c.Assert(err, IsNil)

	event = <-watch
	c.Assert(event.Type, Equals, zk.EVENT_CHANGED)

	c.Check(zk.CountPendingWatches(), Equals, 1)
}

func (s *S) TestGetAndWatchWithError(c *C) {
	c.Check(zk.CountPendingWatches(), Equals, 0)

	conn, _ := s.init(c)

	c.Check(zk.CountPendingWatches(), Equals, 1)

	_, _, watch, err := conn.GetW("/test")
	c.Assert(err, NotNil)
	c.Assert(err, Equals, zk.ZNONODE)
	c.Assert(watch, IsNil)

	c.Check(zk.CountPendingWatches(), Equals, 1)
}

func (s *S) TestCloseReleasesWatches(c *C) {
	c.Check(zk.CountPendingWatches(), Equals, 0)

	conn, _ := s.init(c)

	c.Check(zk.CountPendingWatches(), Equals, 1)

	_, err := conn.Create("/test", "one", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	_, _, _, err = conn.GetW("/test")
	c.Assert(err, IsNil)

	c.Assert(zk.CountPendingWatches(), Equals, 2)

	conn.Close()

	c.Assert(zk.CountPendingWatches(), Equals, 0)
}

// By default, the ZooKeeper C client will hang indefinitely if a
// handler is closed twice.  We get in the way and prevent it.
func (s *S) TestClosingTwiceDoesntHang(c *C) {
	conn, _ := s.init(c)
	err := conn.Close()
	c.Assert(err, IsNil)
	err = conn.Close()
	c.Assert(err, NotNil)
	c.Assert(err, Equals, zk.ZCLOSING)
}

func (s *S) TestChildren(c *C) {
	conn, _ := s.init(c)

	children, stat, err := conn.Children("/")
	c.Assert(err, IsNil)
	c.Assert(children, Equals, []string{"zookeeper"})
	c.Assert(stat.NumChildren(), Equals, int32(1))

	children, stat, err = conn.Children("/non-existent")
	c.Assert(err, NotNil)
	c.Assert(err, Equals, zk.ZNONODE)
	c.Assert(children, Equals, []string{})
	c.Assert(stat, IsNil)
}

func (s *S) TestChildrenAndWatch(c *C) {
	c.Check(zk.CountPendingWatches(), Equals, 0)

	conn, _ := s.init(c)

	c.Check(zk.CountPendingWatches(), Equals, 1)

	children, stat, watch, err := conn.ChildrenW("/")
	c.Assert(err, IsNil)
	c.Assert(children, Equals, []string{"zookeeper"})
	c.Assert(stat.NumChildren(), Equals, int32(1))

	select {
	case <-watch:
		c.Fatal("Watch fired")
	default:
	}

	c.Check(zk.CountPendingWatches(), Equals, 2)

	_, err = conn.Create("/test1", "", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	event := <-watch
	c.Assert(event.Type, Equals, zk.EVENT_CHILD)
	c.Assert(event.Path, Equals, "/")

	c.Check(zk.CountPendingWatches(), Equals, 1)

	children, stat, watch, err = conn.ChildrenW("/")
	c.Assert(err, IsNil)
	c.Assert(stat.NumChildren(), Equals, int32(2))

	// The ordering is most likely unstable, so this test must be fixed.
	c.Assert(children, Equals, []string{"test1", "zookeeper"})

	select {
	case <-watch:
		c.Fatal("Watch fired")
	default:
	}

	c.Check(zk.CountPendingWatches(), Equals, 2)

	_, err = conn.Create("/test2", "", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	event = <-watch
	c.Assert(event.Type, Equals, zk.EVENT_CHILD)

	c.Check(zk.CountPendingWatches(), Equals, 1)
}

func (s *S) TestChildrenAndWatchWithError(c *C) {
	c.Check(zk.CountPendingWatches(), Equals, 0)

	conn, _ := s.init(c)

	c.Check(zk.CountPendingWatches(), Equals, 1)

	_, stat, watch, err := conn.ChildrenW("/test")
	c.Assert(err, NotNil)
	c.Assert(err, Equals, zk.ZNONODE)
	c.Assert(watch, IsNil)
	c.Assert(stat, IsNil)

	c.Check(zk.CountPendingWatches(), Equals, 1)
}

func (s *S) TestExists(c *C) {
	conn, _ := s.init(c)

	stat, err := conn.Exists("/non-existent")
	c.Assert(err, IsNil)
	c.Assert(stat, IsNil)

	stat, err = conn.Exists("/zookeeper")
	c.Assert(err, IsNil)
}

func (s *S) TestExistsAndWatch(c *C) {
	c.Check(zk.CountPendingWatches(), Equals, 0)

	conn, _ := s.init(c)

	c.Check(zk.CountPendingWatches(), Equals, 1)

	stat, watch, err := conn.ExistsW("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, IsNil)

	c.Check(zk.CountPendingWatches(), Equals, 2)

	select {
	case <-watch:
		c.Fatal("Watch fired")
	default:
	}

	_, err = conn.Create("/test", "", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	event := <-watch
	c.Assert(event.Type, Equals, zk.EVENT_CREATED)
	c.Assert(event.Path, Equals, "/test")

	c.Check(zk.CountPendingWatches(), Equals, 1)

	stat, watch, err = conn.ExistsW("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, NotNil)
	c.Assert(stat.NumChildren(), Equals, int32(0))

	c.Check(zk.CountPendingWatches(), Equals, 2)
}

func (s *S) TestExistsAndWatchWithError(c *C) {
	c.Check(zk.CountPendingWatches(), Equals, 0)

	conn, _ := s.init(c)

	c.Check(zk.CountPendingWatches(), Equals, 1)

	stat, watch, err := conn.ExistsW("///")
	c.Assert(err, NotNil)
	c.Assert(err, Equals, zk.ZBADARGUMENTS)
	c.Assert(stat, IsNil)
	c.Assert(watch, IsNil)

	c.Check(zk.CountPendingWatches(), Equals, 1)
}

func (s *S) TestDelete(c *C) {
	conn, _ := s.init(c)

	_, err := conn.Create("/test", "", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	err = conn.Delete("/test", 5)
	c.Assert(err, NotNil)
	c.Assert(err, Equals, zk.ZBADVERSION)

	err = conn.Delete("/test", -1)
	c.Assert(err, IsNil)

	err = conn.Delete("/test", -1)
	c.Assert(err, NotNil)
	c.Assert(err, Equals, zk.ZNONODE)
}

func (s *S) TestClientIdAndReInit(c *C) {
	zk1, _ := s.init(c)
	clientId1 := zk1.ClientId()

	zk2, _, err := zk.Redial(s.zkAddr, 5e9, clientId1)
	c.Assert(err, IsNil)
	defer zk2.Close()
	clientId2 := zk2.ClientId()

	c.Assert(clientId1, Equals, clientId2)
}

// Surprisingly for some (including myself, initially), the watch
// returned by the exists method actually fires on data changes too.
func (s *S) TestExistsWatchOnDataChange(c *C) {
	conn, _ := s.init(c)

	_, err := conn.Create("/test", "", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	_, watch, err := conn.ExistsW("/test")
	c.Assert(err, IsNil)

	_, err = conn.Set("/test", "new", -1)
	c.Assert(err, IsNil)

	event := <-watch

	c.Assert(event.Path, Equals, "/test")
	c.Assert(event.Type, Equals, zk.EVENT_CHANGED)
}

func (s *S) TestACL(c *C) {
	conn, _ := s.init(c)

	_, err := conn.Create("/test", "", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	acl, stat, err := conn.ACL("/test")
	c.Assert(err, IsNil)
	c.Assert(acl, Equals, zk.WorldACL(zk.PERM_ALL))
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, int32(0))

	acl, stat, err = conn.ACL("/non-existent")
	c.Assert(err, NotNil)
	c.Assert(err, Equals, zk.ZNONODE)
	c.Assert(acl, IsNil)
	c.Assert(stat, IsNil)
}

func (s *S) TestSetACL(c *C) {
	conn, _ := s.init(c)

	_, err := conn.Create("/test", "", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	err = conn.SetACL("/test", zk.WorldACL(zk.PERM_ALL), 5)
	c.Assert(err, NotNil)
	c.Assert(err, Equals, zk.ZBADVERSION)

	err = conn.SetACL("/test", zk.WorldACL(zk.PERM_READ), -1)
	c.Assert(err, IsNil)

	acl, _, err := conn.ACL("/test")
	c.Assert(err, IsNil)
	c.Assert(acl, Equals, zk.WorldACL(zk.PERM_READ))
}

func (s *S) TestAddAuth(c *C) {
	conn, _ := s.init(c)

	acl := []zk.ACL{{zk.PERM_READ, "digest", "joe:enQcM3mIEHQx7IrPNStYBc0qfs8="}}

	_, err := conn.Create("/test", "", zk.EPHEMERAL, acl)
	c.Assert(err, IsNil)

	_, _, err = conn.Get("/test")
	c.Assert(err, NotNil)
	c.Assert(err, Equals, zk.ZNOAUTH)

	err = conn.AddAuth("digest", "joe:passwd")
	c.Assert(err, IsNil)

	_, _, err = conn.Get("/test")
	c.Assert(err, IsNil)
}

func (s *S) TestWatchOnReconnection(c *C) {
	c.Check(zk.CountPendingWatches(), Equals, 0)

	conn, session := s.init(c)

	event := <-session
	c.Assert(event.Type, Equals, zk.EVENT_SESSION)
	c.Assert(event.State, Equals, zk.STATE_CONNECTED)

	c.Check(zk.CountPendingWatches(), Equals, 1)

	stat, watch, err := conn.ExistsW("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, IsNil)

	c.Check(zk.CountPendingWatches(), Equals, 2)

	s.StopZK()
	time.Sleep(2e9)
	s.StartZK(c)

	// The session channel should receive the reconnection notification.
	select {
	case event := <-session:
		c.Assert(event.State, Equals, zk.STATE_CONNECTING)
	case <-time.After(3e9):
		c.Fatal("Session watch didn't fire")
	}
	select {
	case event := <-session:
		c.Assert(event.State, Equals, zk.STATE_CONNECTED)
	case <-time.After(3e9):
		c.Fatal("Session watch didn't fire")
	}

	// The watch channel should not, since it's not affected.
	select {
	case event := <-watch:
		c.Fatalf("Exists watch fired: %s", event)
	default:
	}

	// And it should still work.
	_, err = conn.Create("/test", "", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	event = <-watch
	c.Assert(event.Type, Equals, zk.EVENT_CREATED)
	c.Assert(event.Path, Equals, "/test")

	c.Check(zk.CountPendingWatches(), Equals, 1)
}

func (s *S) TestWatchOnSessionExpiration(c *C) {
	c.Check(zk.CountPendingWatches(), Equals, 0)

	conn, session := s.init(c)

	event := <-session
	c.Assert(event.Type, Equals, zk.EVENT_SESSION)
	c.Assert(event.State, Equals, zk.STATE_CONNECTED)

	c.Check(zk.CountPendingWatches(), Equals, 1)

	stat, watch, err := conn.ExistsW("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, IsNil)

	c.Check(zk.CountPendingWatches(), Equals, 2)

	// Use expiration trick described in the FAQ.
	clientId := conn.ClientId()
	zk2, session2, err := zk.Redial(s.zkAddr, 5e9, clientId)

	for event := range session2 {
		c.Log("Event from overlapping session: ", event)
		if event.State == zk.STATE_CONNECTED {
			// Wait for zk to process the connection.
			// Not reliable without this. :-(
			time.Sleep(1e9)
			zk2.Close()
		}
	}
	for event := range session {
		c.Log("Event from primary session: ", event)
		if event.State == zk.STATE_EXPIRED_SESSION {
			break
		}
	}

	select {
	case event := <-watch:
		c.Assert(event.State, Equals, zk.STATE_EXPIRED_SESSION)
	case <-time.After(3e9):
		c.Fatal("Watch event didn't fire")
	}

	event = <-watch
	c.Assert(event.Type, Equals, zk.EVENT_CLOSED)
	c.Assert(event.State, Equals, zk.STATE_CLOSED)

	c.Check(zk.CountPendingWatches(), Equals, 1)
}
