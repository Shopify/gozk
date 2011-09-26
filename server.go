package zk

import (
	"bufio"
	"bytes"
	"exec"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"strconv"
)

type Server struct {
	runDir     string
	installDir string
}

// CreateServer creates the directory runDir and sets up a ZooKepeer server
// environment inside it. It is an error if runDir already exists.
// The server will listen on the specified TCP port.
// 
// The ZooKepeer installation directory is specified by installDir.
// If this is empty, a system default will be used.
//
// CreateServer does not start the server.
func CreateServer(port int, runDir, installDir string) (*Server, os.Error) {
	if err := os.Mkdir(runDir, 0777); err != nil {
		return nil, err
	}
	srv := &Server{runDir: runDir, installDir: installDir}
	if err := srv.writeLog4JConfig(); err != nil {
		return nil, err
	}
	if err := srv.writeZooKeeperConfig(port); err != nil {
		return nil, err
	}
	if err := srv.writeInstallDir(); err != nil {
		return nil, err
	}
	return srv, nil
}

// AttachServer creates a new ZooKeeper Server instance
// to operate inside an existing run directory, runDir.
// The directory must have been created with CreateServer.
func AttachServer(runDir string) (*Server, os.Error) {
	srv := &Server{runDir: runDir}
	if err := srv.readInstallDir(); err != nil {
		return nil, fmt.Errorf("cannot read server install directory: %v", err)
	}
	return srv, nil
}

// readProcess returns a Process referring to the running server from
// where it's been stored in pid.txt. If the file does not
// exist, it assumes the server is not running and returns (nil, nil).
//
func (srv *Server) getServerProcess() (*os.Process, os.Error) {
	data, err := ioutil.ReadFile(srv.path("pid.txt"))
	if err != nil {
		if err, ok := err.(*os.PathError); ok && err.Error == os.ENOENT {
			return nil, nil
		}
		return nil, err
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return nil, os.NewError("bad process id found in pid.txt")
	}
	return os.FindProcess(pid)
}

func (srv *Server) path(name string) string {
	return filepath.Join(srv.runDir, name)
}

// Start starts the ZooKeeper server running.
func (srv *Server) Start() os.Error {
	p, err := srv.getServerProcess()
	if p != nil || err != nil {
		if p != nil {
			p.Release()
		}
		return fmt.Errorf("ZooKeeper server may already be running (remove %q to clear)", srv.path("pid.txt"))
	}

	// open the pid file before starting the process so that if we get two
	// programs trying to concurrently start a server on the same directory
	// at the same time, only one should succeed.
	pidf, err := os.OpenFile(srv.path("pid.txt"), os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("cannot create pid.txt: %v", err)
	}
	defer pidf.Close()

	cp, err := srv.classPath()
	if err != nil {
		return fmt.Errorf("cannot get class path: ", err)
	}
	cmd := exec.Command(
		"java",
		"-cp", strings.Join(cp, ":"),
		"-Dzookeeper.root.logger=INFO,CONSOLE",
		"-Dlog4j.configuration=file:"+srv.path("log4j.properties"),
		"org.apache.zookeeper.server.quorum.QuorumPeerMain",
		srv.path("zoo.cfg"),
	)
	// We open the log file here so that we have one log file for both
	// stdout/stderr messages (e.g. when the class path is incorrect)
	// and for the ZooKeeper log messages themselves.
	logf, err := os.OpenFile(srv.path("log.txt"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("cannot create log file: %v", err)
	}
	defer logf.Close()
	cmd.Stdout = logf
	cmd.Stderr = logf
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("cannot start server: %v", err)
	}
	if _, err := fmt.Fprint(pidf, cmd.Process.Pid); err != nil {
		return fmt.Errorf("cannot write pid file: %v", err)
	}
	return nil
}

// Stop kills the ZooKeeper server. It is a no-op if it is already running
func (srv *Server) Stop() os.Error {
	p, err := srv.getServerProcess()
	if p == nil {
		if err != nil {
			return fmt.Errorf("cannot read process ID of server: %v", err)
		}
		return nil
	}
	defer p.Release()
	if err := p.Kill(); err != nil {
		return fmt.Errorf("cannot kill server process: %v", err)
	}
	// ignore the error returned from Wait because there's little
	// we can do about it - it either means that the process has just exited
	// anyway or something else has happened which probably doesn't
	// have anything to do with stopping the server.
	p.Wait(0)

	if err := os.Remove(srv.path("pid.txt")); err != nil {
		return fmt.Errorf("cannot remove server process ID file: %v", err)
	}
	return nil
}

// Destroy stops the ZooKeeper server if it is running,
// and then removes its run directory and all its contents.
// Warning: this will destroy all data associated with the server.
func (srv *Server) Destroy() os.Error {
	if err := srv.Stop(); err != nil {
		return err
	}
	if err := os.RemoveAll(srv.runDir); err != nil {
		return err
	}
	return nil
}

var log4jProperties = `
log4j.rootLogger=INFO, CONSOLE
log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
log4j.appender.CONSOLE.Threshold=INFO
log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n
`

func (srv *Server) writeLog4JConfig() (err os.Error) {
	return ioutil.WriteFile(srv.path("log4j.properties"), []byte(log4jProperties), 0666)
}

func (srv *Server) writeZooKeeperConfig(port int) (err os.Error) {
	return ioutil.WriteFile(srv.path("zoo.cfg"), []byte(fmt.Sprintf(
		"tickTime=2000\n"+
			"dataDir=%s\n"+
			"clientPort=%d\n"+
			"maxClientCnxns=500\n",
		srv.runDir, port)), 0666)
}

func (srv *Server) writeInstallDir() os.Error {
	return ioutil.WriteFile(srv.path("installdir.txt"), []byte(srv.installDir+"\n"), 0666)
}

func (srv *Server) readInstallDir() os.Error {
	data, err := ioutil.ReadFile(srv.path("installdir.txt"))
	if err != nil {
		return err
	}
	if data[len(data)-1] == '\n' {
		data = data[0 : len(data)-1]
	}
	srv.installDir = string(data)
	return nil
}

func (srv *Server) classPath() ([]string, os.Error) {
	dir := srv.installDir
	if dir == "" {
		return systemClassPath()
	}
	if err := checkDirectory(dir); err != nil {
		return nil, err
	}
	// Two possibilities, as seen in zkEnv.sh:
	// 1) locally built binaries (jars are in build directory)
	// 2) release binaries
	if build := filepath.Join(dir, "build"); checkDirectory(build) == nil {
		dir = build
	}
	classPath, err := filepath.Glob(filepath.Join(dir, "zookeeper-*.jar"))
	if err != nil {
		panic(fmt.Errorf("glob for jar files: %v", err))
	}
	more, err := filepath.Glob(filepath.Join(dir, "lib/*.jar"))
	if err != nil {
		panic(fmt.Errorf("glob for lib jar files: %v", err))
	}

	classPath = append(classPath, more...)
	if len(classPath) == 0 {
		return nil, fmt.Errorf("zookeeper libraries not found in %q", dir)
	}
	return classPath, nil
}

const zookeeperEnviron = "/etc/zookeeper/conf/environment"

func systemClassPath() ([]string, os.Error) {
	f, err := os.Open(zookeeperEnviron)
	if f == nil {
		return nil, err
	}
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			break
		}
		if !bytes.HasPrefix(line, []byte("CLASSPATH=")) {
			continue
		}

		// remove variable and newline
		path := string(line[len("CLASSPATH=") : len(line)-1])

		// trim white space
		path = strings.Trim(path, " \t\r")

		// strip quotes
		if path[0] == '"' {
			path = path[1 : len(path)-1]
		}

		// split on :
		classPath := strings.Split(path, ":")

		// split off $ZOOCFGDIR
		if len(classPath) > 0 && classPath[0] == "$ZOOCFGDIR" {
			classPath = classPath[1:]
		}

		if len(classPath) == 0 {
			return nil, fmt.Errorf("empty class path in %q", zookeeperEnviron)
		}
		return classPath, nil
	}
	return nil, fmt.Errorf("no class path found in %q", zookeeperEnviron)
}

// checkDirectory returns an error if the given path
// does not exist or is not a directory.
func checkDirectory(path string) os.Error {
	if info, err := os.Stat(path); err != nil || !info.IsDirectory() {
		if err == nil {
			err = &os.PathError{Op: "stat", Path: path, Error: os.NewError("is not a directory")}
		}
		return err
	}
	return nil
}
