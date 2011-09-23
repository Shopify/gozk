package zookeeper

import (
	"bufio"
	"bytes"
	"exec"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

const zookeeperEnviron = "/etc/zookeeper/conf/environment"

// Server returns a command that can be used to run a zookeeper
// server listening on the specified TCP port.
// The server is set up to send all log messages to its standard output
// (/dev/null by default).
// The zookeeper installation directory is specified by installedDir.
// If this is empty, a system default will be used.
// All zookeeper instance data will be stored in dataDir, which
// must already exist.
func Server(port int, dataDir, installedDir string) (c *exec.Cmd, err os.Error) {
	cp, err := classPath(installedDir)
	if err != nil {
		return nil, err
	}
	logDir := filepath.Join(dataDir, "log")
	if err = os.Mkdir(logDir, 0777); err != nil && err.(*os.PathError).Error != os.EEXIST {
		return nil, err
	}
	logConfigPath, err := writeLog4JConfig(dataDir)
	if err != nil {
		return nil, err
	}
	configPath, err := writeZookeeperConfig(dataDir, port)
	if err != nil {
		return nil, err
	}
	exe, err := exec.LookPath("java")
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(
		exe,
		"-cp", strings.Join(cp, ":"),
		"-Dzookeeper.log.dir="+logDir,
		"-Dzookeeper.root.logger=INFO,CONSOLE",
		"-Dlog4j.configuration=file:"+logConfigPath,
		"org.apache.zookeeper.server.quorum.QuorumPeerMain",
		configPath,
	)
	return cmd, nil
}

var log4jProperties = []byte(`
log4j.rootLogger=INFO, CONSOLE
log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
log4j.appender.CONSOLE.Threshold=INFO
log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n
`)

func writeLog4JConfig(dir string) (path string, err os.Error) {
	path = filepath.Join(dir, "log4j.properties")
	err = ioutil.WriteFile(path, log4jProperties, 0666)
	return
}

func writeZookeeperConfig(dir string, port int) (path string, err os.Error) {
	path = filepath.Join(dir, "zoo.cfg")
	err = ioutil.WriteFile(path, []byte(fmt.Sprintf(
		"tickTime=2000\n"+
			"dataDir=%s\n"+
			"clientPort=%d\n"+
			"maxClientCnxns=500\n",
		dir, port)), 0666)
	return
}

func classPath(dir string) ([]string, os.Error) {
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
	classPath, _ := filepath.Glob(filepath.Join(dir, "zookeeper-*.jar"))
	more, _ := filepath.Glob(filepath.Join(dir, "lib/*.jar"))
	classPath = append(classPath, more...)
	if len(classPath) == 0 {
		return nil, fmt.Errorf("zookeeper libraries not found in %q", dir)
	}
	return classPath, nil
}

func systemClassPath() ([]string, os.Error) {
	fd, err := os.Open(zookeeperEnviron)
	if fd == nil {
		return nil, err
	}
	r := bufio.NewReader(fd)
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
