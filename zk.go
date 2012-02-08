// gozk - ZooKeeper support for the Go language
//
//   https://wiki.ubuntu.com/gozk
//
// Copyright (c) 2010-2011 Canonical Ltd.
//
// Written by Gustavo Niemeyer <gustavo.niemeyer@canonical.com>
//
package zookeeper

/*
#cgo CFLAGS: -I/usr/include/c-client-src -I/usr/include/zookeeper
#cgo LDFLAGS: -lzookeeper_mt

#include <zookeeper.h>
#include "helpers.h"
*/
import "C"

import (
	"fmt"
	"sync"
	"time"
	"unsafe"
)

// -----------------------------------------------------------------------
// Main constants and data types.

// Conn represents a connection to a set of ZooKeeper nodes.
type Conn struct {
	watchChannels  map[uintptr]chan Event
	sessionWatchId uintptr
	handle         *C.zhandle_t
	mutex          sync.Mutex
}

// ClientId represents an established ZooKeeper session.  It can be
// passed into Redial to reestablish a connection to an existing session.
type ClientId struct {
	cId C.clientid_t
}

// ACL represents one access control list element, providing the permissions
// (one of PERM_*), the scheme ("digest", etc), and the id (scheme-dependent)
// for the access control mechanism in ZooKeeper.
type ACL struct {
	Perms  uint32
	Scheme string
	Id     string
}

// Event channels are used to provide notifications of changes in the
// ZooKeeper connection state and in specific node aspects.
// 
// There are two sources of events: the session channel obtained during
// initialization with Init, and any watch channels obtained
// through one of the W-suffixed functions (GetW, ExistsW, etc).
// 
// The session channel will only receive session-level events notifying
// about critical and transient changes in the ZooKeeper connection
// state (STATE_CONNECTED, STATE_EXPIRED_SESSION, etc).  On long
// running applications the session channel must *necessarily* be
// observed since certain events like session expirations require an
// explicit reconnection and reestablishment of state (or bailing out).
// Because of that, the buffer used on the session channel has a limited
// size, and a panic will occur if too many events are not collected.
//
// Watch channels enable monitoring state for nodes, and the
// moment they're fired depends on which function was called to
// create them.  Note that, unlike in other ZooKeeper interfaces,
// gozk will NOT dispatch unimportant session events such as
// STATE_ASSOCIATING, STATE_CONNECTING and STATE_CONNECTED to
// watch Event channels, since they are transient and disruptive
// to the workflow.  Critical state changes such as expirations
// are still delivered to all event channels, though, and the
// transient events may be obsererved in the session channel.
//
// Since every watch channel may receive critical session events, events
// received must not be handled blindly as if the watch requested has
// been fired.  To facilitate such tests, Events offer the Ok method,
// and they also have a good String method so they may be used as an
// os.Error value if wanted. E.g.:
//
//     event := <-watch
//     if !event.Ok() {
//         err = event
//         return
//     }
//
// Note that closed channels will deliver zeroed Event, which means
// event.Type is set to EVENT_CLOSED and event.State is set to STATE_CLOSED,
// to facilitate handling.
type Event struct {
	Type  int
	Path  string
	State int
}

// Error represents a ZooKeeper error.
type Error int

const (
	ZOK                      Error = C.ZOK
	ZSYSTEMERROR             Error = C.ZSYSTEMERROR
	ZRUNTIMEINCONSISTENCY    Error = C.ZRUNTIMEINCONSISTENCY
	ZDATAINCONSISTENCY       Error = C.ZDATAINCONSISTENCY
	ZCONNECTIONLOSS          Error = C.ZCONNECTIONLOSS
	ZMARSHALLINGERROR        Error = C.ZMARSHALLINGERROR
	ZUNIMPLEMENTED           Error = C.ZUNIMPLEMENTED
	ZOPERATIONTIMEOUT        Error = C.ZOPERATIONTIMEOUT
	ZBADARGUMENTS            Error = C.ZBADARGUMENTS
	ZINVALIDSTATE            Error = C.ZINVALIDSTATE
	ZAPIERROR                Error = C.ZAPIERROR
	ZNONODE                  Error = C.ZNONODE
	ZNOAUTH                  Error = C.ZNOAUTH
	ZBADVERSION              Error = C.ZBADVERSION
	ZNOCHILDRENFOREPHEMERALS Error = C.ZNOCHILDRENFOREPHEMERALS
	ZNODEEXISTS              Error = C.ZNODEEXISTS
	ZNOTEMPTY                Error = C.ZNOTEMPTY
	ZSESSIONEXPIRED          Error = C.ZSESSIONEXPIRED
	ZINVALIDCALLBACK         Error = C.ZINVALIDCALLBACK
	ZINVALIDACL              Error = C.ZINVALIDACL
	ZAUTHFAILED              Error = C.ZAUTHFAILED
	ZCLOSING                 Error = C.ZCLOSING
	ZNOTHING                 Error = C.ZNOTHING
	ZSESSIONMOVED            Error = C.ZSESSIONMOVED
)

func (error Error) Error() string {
	return C.GoString(C.zerror(C.int(error))) // Static, no need to free it.
}

// zkError creates an appropriate error return from
// a ZooKeeper status and the errno return from a C API
// call.
func zkError(rc C.int, cerr error) error {
	code := Error(rc)
	switch code {
	case ZOK:
		return nil

	case ZSYSTEMERROR:
		// If a ZooKeeper call returns ZSYSTEMERROR, then
		// errno becomes significant. If errno has not been
		// set, then we will return ZSYSTEMERROR nonetheless.
		if cerr != nil {
			return cerr
		}
	}
	return code
}

// Constants for SetLogLevel.
const (
	LOG_ERROR = C.ZOO_LOG_LEVEL_ERROR
	LOG_WARN  = C.ZOO_LOG_LEVEL_WARN
	LOG_INFO  = C.ZOO_LOG_LEVEL_INFO
	LOG_DEBUG = C.ZOO_LOG_LEVEL_DEBUG
)

// These are defined as extern.  To avoid having to declare them as
// variables here they are inlined, and correctness is ensured on
// init().

// Constants for Create's flags parameter.
const (
	EPHEMERAL = 1 << iota
	SEQUENCE
)

// Constants for ACL Perms.
const (
	PERM_READ = 1 << iota
	PERM_WRITE
	PERM_CREATE
	PERM_DELETE
	PERM_ADMIN
	PERM_ALL = 0x1f
)

// Constants for Event Type.
const (
	EVENT_CREATED = iota + 1
	EVENT_DELETED
	EVENT_CHANGED
	EVENT_CHILD
	EVENT_SESSION     = -1
	EVENT_NOTWATCHING = -2

	// Doesn't really exist in zk, but handy for use in zeroed Event
	// values (e.g. closed channels).
	EVENT_CLOSED = 0
)

// Constants for Event State.
const (
	STATE_EXPIRED_SESSION = -112
	STATE_AUTH_FAILED     = -113
	STATE_CONNECTING      = 1
	STATE_ASSOCIATING     = 2
	STATE_CONNECTED       = 3

	// Doesn't really exist in zk, but handy for use in zeroed Event
	// values (e.g. closed channels).
	STATE_CLOSED = 0
)

func init() {
	if EPHEMERAL != C.ZOO_EPHEMERAL ||
		SEQUENCE != C.ZOO_SEQUENCE ||
		PERM_READ != C.ZOO_PERM_READ ||
		PERM_WRITE != C.ZOO_PERM_WRITE ||
		PERM_CREATE != C.ZOO_PERM_CREATE ||
		PERM_DELETE != C.ZOO_PERM_DELETE ||
		PERM_ADMIN != C.ZOO_PERM_ADMIN ||
		PERM_ALL != C.ZOO_PERM_ALL ||
		EVENT_CREATED != C.ZOO_CREATED_EVENT ||
		EVENT_DELETED != C.ZOO_DELETED_EVENT ||
		EVENT_CHANGED != C.ZOO_CHANGED_EVENT ||
		EVENT_CHILD != C.ZOO_CHILD_EVENT ||
		EVENT_SESSION != C.ZOO_SESSION_EVENT ||
		EVENT_NOTWATCHING != C.ZOO_NOTWATCHING_EVENT ||
		STATE_EXPIRED_SESSION != C.ZOO_EXPIRED_SESSION_STATE ||
		STATE_AUTH_FAILED != C.ZOO_AUTH_FAILED_STATE ||
		STATE_CONNECTING != C.ZOO_CONNECTING_STATE ||
		STATE_ASSOCIATING != C.ZOO_ASSOCIATING_STATE ||
		STATE_CONNECTED != C.ZOO_CONNECTED_STATE {

		panic("OOPS: Constants don't match C counterparts")
	}
	SetLogLevel(0)
}

// AuthACL produces an ACL list containing a single ACL which uses
// the provided permissions, with the scheme "auth", and ID "", which
// is used by ZooKeeper to represent any authenticated user.
func AuthACL(perms uint32) []ACL {
	return []ACL{{perms, "auth", ""}}
}

// WorldACL produces an ACL list containing a single ACL which uses
// the provided permissions, with the scheme "world", and ID "anyone",
// which is used by ZooKeeper to represent any user at all.
func WorldACL(perms uint32) []ACL {
	return []ACL{{perms, "world", "anyone"}}
}

// -----------------------------------------------------------------------
// Event methods.

// Ok returns true in case the event reports zk as being in a usable state.
func (e Event) Ok() bool {
	// That's really it for now. Anything else seems to mean zk
	// can't be used at the moment.
	return e.State == STATE_CONNECTED
}

func (e Event) String() (s string) {
	switch e.State {
	case STATE_EXPIRED_SESSION:
		s = "ZooKeeper session expired"
	case STATE_AUTH_FAILED:
		s = "ZooKeeper authentication failed"
	case STATE_CONNECTING:
		s = "ZooKeeper connecting"
	case STATE_ASSOCIATING:
		s = "ZooKeeper still associating"
	case STATE_CONNECTED:
		s = "ZooKeeper connected"
	case STATE_CLOSED:
		s = "ZooKeeper connection closed"
	default:
		s = fmt.Sprintf("unknown ZooKeeper state %d", e.State)
	}
	if e.Type == -1 || e.Type == EVENT_SESSION {
		return
	}
	if s != "" {
		s += "; "
	}
	switch e.Type {
	case EVENT_CREATED:
		s += "path created: "
	case EVENT_DELETED:
		s += "path deleted: "
	case EVENT_CHANGED:
		s += "path changed: "
	case EVENT_CHILD:
		s += "path children changed: "
	case EVENT_NOTWATCHING:
		s += "not watching: " // !?
	case EVENT_SESSION:
		// nothing
	}
	s += e.Path
	return
}

// -----------------------------------------------------------------------

// Stat contains detailed information about a node.
type Stat struct {
	c C.struct_Stat
}

func (stat *Stat) Czxid() int64 {
	return int64(stat.c.czxid)
}

func (stat *Stat) Mzxid() int64 {
	return int64(stat.c.mzxid)
}

func (stat *Stat) CTime() int64 {
	return int64(stat.c.ctime)
}

func (stat *Stat) MTime() int64 {
	return int64(stat.c.mtime)
}

func (stat *Stat) Version() int32 {
	return int32(stat.c.version)
}

func (stat *Stat) CVersion() int32 {
	return int32(stat.c.cversion)
}

func (stat *Stat) AVersion() int32 {
	return int32(stat.c.aversion)
}

func (stat *Stat) EphemeralOwner() int64 {
	return int64(stat.c.ephemeralOwner)
}

func (stat *Stat) DataLength() int32 {
	return int32(stat.c.dataLength)
}

func (stat *Stat) NumChildren() int32 {
	return int32(stat.c.numChildren)
}

func (stat *Stat) Pzxid() int64 {
	return int64(stat.c.pzxid)
}

// -----------------------------------------------------------------------
// Functions and methods related to ZooKeeper itself.

const bufferSize = 1024 * 1024

// SetLogLevel changes the minimum level of logging output generated
// to adjust the amount of information provided.
func SetLogLevel(level int) {
	C.zoo_set_debug_level(C.ZooLogLevel(level))
}

// Dial initializes the communication with a ZooKeeper cluster. The provided
// servers parameter may include multiple server addresses, separated
// by commas, so that the client will automatically attempt to connect
// to another server if one of them stops working for whatever reason.
//
// The recvTimeout parameter, given in nanoseconds, allows controlling
// the amount of time the connection can stay unresponsive before the
// server will be considered problematic.
//
// Session establishment is asynchronous, meaning that this function
// will return before the communication with ZooKeeper is fully established.
// The watch channel receives events of type SESSION_EVENT when any change
// to the state of the established connection happens.  See the documentation
// for the Event type for more details.
func Dial(servers string, recvTimeout time.Duration) (*Conn, <-chan Event, error) {
	return dial(servers, recvTimeout, nil)
}

// Redial is equivalent to Dial, but attempts to reestablish an existing session
// identified via the clientId parameter.
func Redial(servers string, recvTimeout time.Duration, clientId *ClientId) (*Conn, <-chan Event, error) {
	return dial(servers, recvTimeout, clientId)
}

func dial(servers string, recvTimeout time.Duration, clientId *ClientId) (*Conn, <-chan Event, error) {
	conn := &Conn{}
	conn.watchChannels = make(map[uintptr]chan Event)

	var cId *C.clientid_t
	if clientId != nil {
		cId = &clientId.cId
	}

	watchId, watchChannel := conn.createWatch(true)
	conn.sessionWatchId = watchId

	cservers := C.CString(servers)
	handle, cerr := C.zookeeper_init(cservers, C.watch_handler, C.int(recvTimeout/1e6), cId, unsafe.Pointer(watchId), 0)
	C.free(unsafe.Pointer(cservers))
	if handle == nil {
		conn.closeAllWatches()
		return nil, nil, zkError(C.int(ZSYSTEMERROR), cerr)
	}
	conn.handle = handle
	runWatchLoop()
	return conn, watchChannel, nil
}

// ClientId returns the client ID for the existing session with ZooKeeper.
// This is useful to reestablish an existing session via ReInit.
func (conn *Conn) ClientId() *ClientId {
	return &ClientId{*C.zoo_client_id(conn.handle)}
}

// Close terminates the ZooKeeper interaction.
func (conn *Conn) Close() error {

	// Protect from concurrency around conn.handle change.
	conn.mutex.Lock()
	defer conn.mutex.Unlock()

	if conn.handle == nil {
		// ZooKeeper may hang indefinitely if a handler is closed twice,
		// so we get in the way and prevent it from happening.
		return ZCLOSING
	}
	rc, cerr := C.zookeeper_close(conn.handle)

	conn.closeAllWatches()
	stopWatchLoop()

	// At this point, nothing else should need conn.handle.
	conn.handle = nil

	return zkError(rc, cerr)
}

// Get returns the data and status from an existing node.  err will be nil,
// unless an error is found. Attempting to retrieve data from a non-existing
// node is an error.
func (conn *Conn) Get(path string) (data string, stat *Stat, err error) {

	cpath := C.CString(path)
	cbuffer := (*C.char)(C.malloc(bufferSize))
	cbufferLen := C.int(bufferSize)
	defer C.free(unsafe.Pointer(cpath))
	defer C.free(unsafe.Pointer(cbuffer))

	var cstat Stat
	rc, cerr := C.zoo_wget(conn.handle, cpath, nil, nil, cbuffer, &cbufferLen, &cstat.c)
	if rc != C.ZOK {
		return "", nil, zkError(rc, cerr)
	}

	result := C.GoStringN(cbuffer, cbufferLen)
	return result, &cstat, nil
}

// GetW works like Get but also returns a channel that will receive
// a single Event value when the data or existence of the given ZooKeeper
// node changes or when critical session events happen.  See the
// documentation of the Event type for more details.
func (conn *Conn) GetW(path string) (data string, stat *Stat, watch <-chan Event, err error) {

	cpath := C.CString(path)
	cbuffer := (*C.char)(C.malloc(bufferSize))
	cbufferLen := C.int(bufferSize)
	defer C.free(unsafe.Pointer(cpath))
	defer C.free(unsafe.Pointer(cbuffer))

	watchId, watchChannel := conn.createWatch(true)

	var cstat Stat
	rc, cerr := C.zoo_wget(conn.handle, cpath, C.watch_handler, unsafe.Pointer(watchId), cbuffer, &cbufferLen, &cstat.c)
	if rc != C.ZOK {
		conn.forgetWatch(watchId)
		return "", nil, nil, zkError(rc, cerr)
	}

	result := C.GoStringN(cbuffer, cbufferLen)
	return result, &cstat, watchChannel, nil
}

// Children returns the children list and status from an existing node.
// Attempting to retrieve the children list from a non-existent node is an error.
func (conn *Conn) Children(path string) (children []string, stat *Stat, err error) {

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	cvector := C.struct_String_vector{}
	var cstat Stat
	rc, cerr := C.zoo_wget_children2(conn.handle, cpath, nil, nil, &cvector, &cstat.c)

	// Can't happen if rc != 0, but avoid potential memory leaks in the future.
	if cvector.count != 0 {
		children = parseStringVector(&cvector)
	}
	if rc == C.ZOK {
		stat = &cstat
	} else {
		err = zkError(rc, cerr)
	}
	return
}

// ChildrenW works like Children but also returns a channel that will
// receive a single Event value when a node is added or removed under the
// provided path or when critical session events happen.  See the documentation
// of the Event type for more details.
func (conn *Conn) ChildrenW(path string) (children []string, stat *Stat, watch <-chan Event, err error) {

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	watchId, watchChannel := conn.createWatch(true)

	cvector := C.struct_String_vector{}
	var cstat Stat
	rc, cerr := C.zoo_wget_children2(conn.handle, cpath, C.watch_handler, unsafe.Pointer(watchId), &cvector, &cstat.c)

	// Can't happen if rc != 0, but avoid potential memory leaks in the future.
	if cvector.count != 0 {
		children = parseStringVector(&cvector)
	}
	if rc == C.ZOK {
		stat = &cstat
		watch = watchChannel
	} else {
		conn.forgetWatch(watchId)
		err = zkError(rc, cerr)
	}
	return
}

func parseStringVector(cvector *C.struct_String_vector) []string {
	vector := make([]string, cvector.count)
	dataStart := uintptr(unsafe.Pointer(cvector.data))
	uintptrSize := unsafe.Sizeof(dataStart)
	for i := 0; i != len(vector); i++ {
		cpathPos := dataStart + uintptr(i)*uintptrSize
		cpath := *(**C.char)(unsafe.Pointer(cpathPos))
		vector[i] = C.GoString(cpath)
	}
	C.deallocate_String_vector(cvector)
	return vector
}

// Exists checks if a node exists at the given path.  If it does,
// stat will contain meta information on the existing node, otherwise
// it will be nil.
func (conn *Conn) Exists(path string) (stat *Stat, err error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	var cstat Stat
	rc, cerr := C.zoo_wexists(conn.handle, cpath, nil, nil, &stat.c)

	// We diverge a bit from the usual here: a ZNONODE is not an error
	// for an exists call, otherwise every Exists call would have to check
	// for err != nil and err.Code() != ZNONODE.
	if rc == C.ZOK {
		stat = &cstat
	} else if rc != C.ZNONODE {
		err = zkError(rc, cerr)
	}
	return
}

// ExistsW works like Exists but also returns a channel that will
// receive an Event value when a node is created in case the returned
// stat is nil and the node didn't exist, or when the existing node
// is removed. It will also receive critical session events. See the
// documentation of the Event type for more details.
func (conn *Conn) ExistsW(path string) (stat *Stat, watch <-chan Event, err error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	watchId, watchChannel := conn.createWatch(true)

	var cstat Stat
	rc, cerr := C.zoo_wexists(conn.handle, cpath, C.watch_handler, unsafe.Pointer(watchId), &cstat.c)

	// We diverge a bit from the usual here: a ZNONODE is not an error
	// for an exists call, otherwise every Exists call would have to check
	// for err != nil and err.Code() != ZNONODE.
	switch Error(rc) {
	case ZOK:
		stat = &cstat
		watch = watchChannel
	case ZNONODE:
		watch = watchChannel
	default:
		conn.forgetWatch(watchId)
		err = zkError(rc, cerr)
	}
	return
}

// Create creates a node at the given path with the given data. The
// provided flags may determine features such as whether the node is
// ephemeral or not, or whether it should have a sequence number
// attached to it, and the provided ACLs will determine who can access
// the node and under which circumstances.
//
// The returned path is useful in cases where the created path may differ
// from the requested one, such as when a sequence number is appended
// to it due to the use of the gozk.SEQUENCE flag.
func (conn *Conn) Create(path, value string, flags int, aclv []ACL) (pathCreated string, err error) {
	cpath := C.CString(path)
	cvalue := C.CString(value)
	defer C.free(unsafe.Pointer(cpath))
	defer C.free(unsafe.Pointer(cvalue))

	caclv := buildACLVector(aclv)
	defer C.deallocate_ACL_vector(caclv)

	// Allocate additional space for the sequence (10 bytes should be enough).
	cpathLen := C.size_t(len(path) + 32)
	cpathCreated := (*C.char)(C.malloc(cpathLen))
	defer C.free(unsafe.Pointer(cpathCreated))

	rc, cerr := C.zoo_create(conn.handle, cpath, cvalue, C.int(len(value)), caclv, C.int(flags), cpathCreated, C.int(cpathLen))
	if rc == C.ZOK {
		pathCreated = C.GoString(cpathCreated)
	} else {
		err = zkError(rc, cerr)
	}
	return
}

// Set modifies the data for the existing node at the given path, replacing it
// by the provided value. If version is not -1, the operation will only
// succeed if the node is still at the given version when the replacement
// happens as an atomic operation. The returned Stat value will contain
// data for the resulting node, after the operation is performed.
//
// It is an error to attempt to set the data of a non-existing node with
// this function. In these cases, use Create instead.
func (conn *Conn) Set(path, value string, version int32) (stat *Stat, err error) {

	cpath := C.CString(path)
	cvalue := C.CString(value)
	defer C.free(unsafe.Pointer(cpath))
	defer C.free(unsafe.Pointer(cvalue))

	var cstat Stat
	rc, cerr := C.zoo_set2(conn.handle, cpath, cvalue, C.int(len(value)), C.int(version), &cstat.c)
	if rc == C.ZOK {
		stat = &cstat
	} else {
		err = zkError(rc, cerr)
	}
	return
}

// Delete removes the node at path. If version is not -1, the operation
// will only succeed if the node is still at this version when the
// node is deleted as an atomic operation.
func (conn *Conn) Delete(path string, version int32) (err error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	rc, cerr := C.zoo_delete(conn.handle, cpath, C.int(version))
	return zkError(rc, cerr)
}

// AddAuth adds a new authentication certificate to the ZooKeeper
// interaction. The scheme parameter will specify how to handle the
// authentication information, while the cert parameter provides the
// identity data itself. For instance, the "digest" scheme requires
// a pair like "username:password" to be provided as the certificate.
func (conn *Conn) AddAuth(scheme, cert string) error {
	cscheme := C.CString(scheme)
	ccert := C.CString(cert)
	defer C.free(unsafe.Pointer(cscheme))
	defer C.free(unsafe.Pointer(ccert))

	data := C.create_completion_data()
	if data == nil {
		panic("Failed to create completion data")
	}
	defer C.destroy_completion_data(data)

	rc, cerr := C.zoo_add_auth(conn.handle, cscheme, ccert, C.int(len(cert)), C.handle_void_completion, unsafe.Pointer(data))
	if rc != C.ZOK {
		return zkError(rc, cerr)
	}

	C.wait_for_completion(data)

	rc = C.int(uintptr(data.data))
	return zkError(rc, nil)
}

// ACL returns the access control list for path.
func (conn *Conn) ACL(path string) ([]ACL, *Stat, error) {

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	caclv := C.struct_ACL_vector{}

	var cstat Stat
	rc, cerr := C.zoo_get_acl(conn.handle, cpath, &caclv, &cstat.c)
	if rc != C.ZOK {
		return nil, nil, zkError(rc, cerr)
	}

	aclv := parseACLVector(&caclv)

	return aclv, &cstat, nil
}

// SetACL changes the access control list for path.
func (conn *Conn) SetACL(path string, aclv []ACL, version int32) error {

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	caclv := buildACLVector(aclv)
	defer C.deallocate_ACL_vector(caclv)

	rc, cerr := C.zoo_set_acl(conn.handle, cpath, C.int(version), caclv)
	return zkError(rc, cerr)
}

func parseACLVector(caclv *C.struct_ACL_vector) []ACL {
	structACLSize := unsafe.Sizeof(C.struct_ACL{})
	aclv := make([]ACL, caclv.count)
	dataStart := uintptr(unsafe.Pointer(caclv.data))
	for i := 0; i != int(caclv.count); i++ {
		caclPos := dataStart + uintptr(i)*structACLSize
		cacl := (*C.struct_ACL)(unsafe.Pointer(caclPos))

		acl := &aclv[i]
		acl.Perms = uint32(cacl.perms)
		acl.Scheme = C.GoString(cacl.id.scheme)
		acl.Id = C.GoString(cacl.id.id)
	}
	C.deallocate_ACL_vector(caclv)

	return aclv
}

func buildACLVector(aclv []ACL) *C.struct_ACL_vector {
	structACLSize := unsafe.Sizeof(C.struct_ACL{})
	data := C.calloc(C.size_t(len(aclv)), C.size_t(structACLSize))
	if data == nil {
		panic("ACL data allocation failed")
	}

	caclv := &C.struct_ACL_vector{}
	caclv.data = (*C.struct_ACL)(data)
	caclv.count = C.int32_t(len(aclv))

	dataStart := uintptr(unsafe.Pointer(caclv.data))
	for i, acl := range aclv {
		caclPos := dataStart + uintptr(i)*structACLSize
		cacl := (*C.struct_ACL)(unsafe.Pointer(caclPos))
		cacl.perms = C.int32_t(acl.Perms)
		// C.deallocate_ACL_vector() will also handle deallocation of these.
		cacl.id.scheme = C.CString(acl.Scheme)
		cacl.id.id = C.CString(acl.Id)
	}

	return caclv
}

// -----------------------------------------------------------------------
// RetryChange utility method.

type ChangeFunc func(oldValue string, oldStat *Stat) (newValue string, err error)

// RetryChange runs changeFunc to attempt to atomically change path
// in a lock free manner, and retries in case there was another
// concurrent change between reading and writing the node.
//
// changeFunc must work correctly if called multiple times in case
// the modification fails due to concurrent changes, and it may return
// an error that will cause the the RetryChange function to stop and
// return the same error.
//
// This mechanism is not suitable for a node that is frequently modified
// concurrently. For those cases, consider using a pessimistic locking
// mechanism.
//
// This is the detailed operation flow for RetryChange:
//
// 1. Attempt to read the node. In case the node exists, but reading it
// fails, stop and return the error found.
//
// 2. Call the changeFunc with the current node value and stat,
// or with an empty string and nil stat, if the node doesn't yet exist.
// If the changeFunc returns an error, stop and return the same error.
//
// 3. If the changeFunc returns no errors, use the string returned as
// the new candidate value for the node, and attempt to either create
// the node, if it didn't exist, or to change its contents at the specified
// version.  If this procedure fails due to conflicts (concurrent changes
// in the same node), repeat from step 1.  If this procedure fails with any
// other error, stop and return the error found.
//
func (conn *Conn) RetryChange(path string, flags int, acl []ACL, changeFunc ChangeFunc) error {
	for {
		oldValue, oldStat, err := conn.Get(path)
		if err != nil && err != ZNONODE {
			return err
		}
		newValue, err := changeFunc(oldValue, oldStat)
		if err != nil {
			return err
		}
		if oldStat == nil {
			_, err := conn.Create(path, newValue, flags, acl)
			if err == nil || err != ZNODEEXISTS {
				return err
			}
			continue
		}
		if newValue == oldValue {
			return nil // Nothing to do.
		}
		_, err = conn.Set(path, newValue, oldStat.Version())
		if err == nil || (err != ZBADVERSION && err != ZNONODE) {
			return err
		}
	}
	panic("not reached")
}

// -----------------------------------------------------------------------
// Watching mechanism.

// The bridging of watches into Go is slightly tricky because Cgo doesn't
// yet provide a nice way to callback from C into a Go routine, so we do
// this by hand.  That bridging works the following way:
//
// Whenever a *W method is called, it will return a channel which
// outputs Event values.  Internally, a map is used to maintain references
// between an unique integer key (the watchId), and the event channel. The
// watchId is then handed to the C ZooKeeper library as the watch context,
// so that we get it back when events happen.  Using an integer key as the
// watch context rather than a pointer is needed because there's no guarantee
// that in the future the GC will not move objects around, and also because
// a strong reference is needed on the Go side so that the channel is not
// garbage-collected.
//
// So, this is what's done to establish the watch.  The interesting part
// lies in the other side of this logic, when events actually happen.
//
// Since Cgo doesn't allow calling back into Go, we actually fire a new
// goroutine the very first time Init is called, and allow it to block
// in a pthread condition variable within a C function. This condition
// will only be notified once a ZooKeeper watch callback appends new
// entries to the event list.  When this happens, the C function returns
// and we get back into Go land with the pointer to the watch data,
// including the watchId and other event details such as type and path.

var watchMutex sync.Mutex
var watchConns = make(map[uintptr]*Conn)
var watchCounter uintptr
var watchLoopCounter int

// CountPendingWatches returns the number of pending watches which have
// not been fired yet, across all ZooKeeper instances.  This is useful
// mostly as a debugging and testing aid.
func CountPendingWatches() int {
	watchMutex.Lock()
	count := len(watchConns)
	watchMutex.Unlock()
	return count
}

// createWatch creates and registers a watch, returning the watch id
// and channel.
func (conn *Conn) createWatch(session bool) (watchId uintptr, watchChannel chan Event) {
	buf := 1 // session/watch event
	if session {
		buf = 32
	}
	watchChannel = make(chan Event, buf)
	watchMutex.Lock()
	defer watchMutex.Unlock()
	watchId = watchCounter
	watchCounter += 1
	conn.watchChannels[watchId] = watchChannel
	watchConns[watchId] = conn
	return
}

// forgetWatch cleans resources used by watchId and prevents it
// from ever getting delivered. It shouldn't be used if there's any
// chance the watch channel is still visible and not closed, since
// it might mean a goroutine would be blocked forever.
func (conn *Conn) forgetWatch(watchId uintptr) {
	watchMutex.Lock()
	defer watchMutex.Unlock()
	delete(conn.watchChannels, watchId)
	delete(watchConns, watchId)
}

// closeAllWatches closes all watch channels for conn.
func (conn *Conn) closeAllWatches() {
	watchMutex.Lock()
	defer watchMutex.Unlock()
	for watchId, ch := range conn.watchChannels {
		close(ch)
		delete(conn.watchChannels, watchId)
		delete(watchConns, watchId)
	}
}

// sendEvent delivers the event to the watchId event channel.  If the
// event channel is a watch event channel, the event is delivered,
// the channel is closed, and resources are freed.
func sendEvent(watchId uintptr, event Event) {
	if event.State == STATE_CLOSED {
		panic("Attempted to send a CLOSED event")
	}
	watchMutex.Lock()
	defer watchMutex.Unlock()
	conn, ok := watchConns[watchId]
	if !ok {
		return
	}
	if event.Type == EVENT_SESSION && watchId != conn.sessionWatchId {
		switch event.State {
		case STATE_EXPIRED_SESSION, STATE_AUTH_FAILED:
		default:
			// WTF? Feels like TCP saying "dropped a dup packet, ok?"
			return
		}
	}
	ch := conn.watchChannels[watchId]
	if ch == nil {
		return
	}
	select {
	case ch <- event:
	default:
		// Channel not available for sending, which means session
		// events are necessarily involved (trivial events go
		// straight to the buffer), and the application isn't paying
		// attention for long enough to have the buffer filled up.
		// Break down now rather than leaking forever.
		if watchId == conn.sessionWatchId {
			panic("Session event channel buffer is full")
		} else {
			panic("Watch event channel buffer is full")
		}
	}
	if watchId != conn.sessionWatchId {
		delete(conn.watchChannels, watchId)
		delete(watchConns, watchId)
		close(ch)
	}
}

// runWatchLoop start the event loop to collect events from the C
// library and dispatch them into Go land.  Calling this function
// multiple times will only increase a counter, rather than
// getting multiple watch loops running.
func runWatchLoop() {
	watchMutex.Lock()
	if watchLoopCounter == 0 {
		go _watchLoop()
	}
	watchLoopCounter += 1
	watchMutex.Unlock()
}

// stopWatchLoop decrements the event loop counter. For the moment,
// the event loop doesn't actually stop, but some day we can easily
// implement termination of the loop if necessary.
func stopWatchLoop() {
	watchMutex.Lock()
	watchLoopCounter -= 1
	if watchLoopCounter == 0 {
		// Not really stopping right now, so let's just
		// avoid it from running again.
		watchLoopCounter += 1
	}
	watchMutex.Unlock()
}

// Loop and block in a C call waiting for a watch to be fired.  When
// it fires, handle the watch by dispatching it to the correct event
// channel, and go back onto waiting mode.
func _watchLoop() {
	for {
		// This will block until there's a watch event is available.
		data := C.wait_for_watch()
		event := Event{
			Type:  int(data.event_type),
			Path:  C.GoString(data.event_path),
			State: int(data.connection_state),
		}
		watchId := uintptr(data.watch_context)
		C.destroy_watch_data(data)
		sendEvent(watchId, event)
	}
}
