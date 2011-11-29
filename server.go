package zk

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const zookeeperEnviron = "/etc/zookeeper/conf/environment"

// Server sets up a ZooKepeer server environment inside dataDir
// for a server that listens on the specified TCP port, sending
// all log messages to standard output.
// The dataDir directory must exist already.
// 
// The ZooKepeer installation directory is specified by installedDir.
// If this is empty, a system default will be used.
//
// Server does not actually start the server. Instead it returns
// a command line, suitable for passing to exec.Command,
// for example.
func Server(port int, dataDir, installedDir string) ([]string, error) {
	cp, err := classPath(installedDir)
	if err != nil {
		return nil, err
	}
	logDir := filepath.Join(dataDir, "log")
	if err = os.Mkdir(logDir, 0777); err != nil && err.(*os.PathError).Err != os.EEXIST {
		return nil, err
	}
	logConfigPath, err := writeLog4JConfig(dataDir)
	if err != nil {
		return nil, err
	}
	configPath, err := writeZooKeeperConfig(dataDir, port)
	if err != nil {
		return nil, err
	}
	exe, err := exec.LookPath("java")
	if err != nil {
		return nil, err
	}
	cmd := []string{
		exe,
		"-cp", strings.Join(cp, ":"),
		"-Dzookeeper.log.dir=" + logDir,
		"-Dzookeeper.root.logger=INFO,CONSOLE",
		"-Dlog4j.configuration=file:" + logConfigPath,
		"org.apache.zookeeper.server.quorum.QuorumPeerMain",
		configPath,
	}
	return cmd, nil
}

var log4jProperties = `
log4j.rootLogger=INFO, CONSOLE
log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
log4j.appender.CONSOLE.Threshold=INFO
log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n
`

func writeLog4JConfig(dir string) (path string, err error) {
	path = filepath.Join(dir, "log4j.properties")
	err = ioutil.WriteFile(path, []byte(log4jProperties), 0666)
	return
}

func writeZooKeeperConfig(dir string, port int) (path string, err error) {
	path = filepath.Join(dir, "zoo.cfg")
	err = ioutil.WriteFile(path, []byte(fmt.Sprintf(
		"tickTime=2000\n"+
			"dataDir=%s\n"+
			"clientPort=%d\n"+
			"maxClientCnxns=500\n",
		dir, port)), 0666)
	return
}

func classPath(dir string) ([]string, error) {
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

func systemClassPath() ([]string, error) {
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
func checkDirectory(path string) error {
	if info, err := os.Stat(path); err != nil || !info.IsDirectory() {
		if err == nil {
			err = &os.PathError{Op: "stat", Path: path, Err: errors.New("is not a directory")}
		}
		return err
	}
	return nil
}
