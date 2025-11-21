package zk

import (
	"ergo.services/ergo/gen"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"strconv"
	"strings"
)

func intToStr(i int) string {
	return strconv.FormatInt(int64(i), 10)
}

func strToInt(s string) int {
	i, _ := strconv.ParseInt(s, 10, 64)
	return int(i)
}

func parseSeq(path string) (int, error) {
	parts := strings.Split(path, "-")
	if len(parts) == 1 {
		parts = strings.Split(path, "__")
	}
	return strconv.Atoi(parts[len(parts)-1])
}

func stringContains(list []string, str string) bool {
	for _, v := range list {
		if v == str {
			return true
		}
	}
	return false
}

func getRuntimeStack() []byte {
	const size = 64 << 10
	buf := make([]byte, size)
	return buf[:runtime.Stack(buf, false)]
}

type CloseChan struct {
	closeOnce sync.Once
	ch        chan struct{}
	closed    atomic.Bool
}

func NewCloseChan() *CloseChan {
	return &CloseChan{ch: make(chan struct{})}
}

func (c *CloseChan) Close(f func()) {
	c.closeOnce.Do(func() {
		if f != nil {
			f()
		}
		close(c.ch)
		c.closed.Store(true)
	})
}

func (c *CloseChan) C() <-chan struct{} {
	return c.ch
}

func (c *CloseChan) Closed() bool {
	return c.closed.Load()
}

type sequences []int

func (s sequences) Min() int {
	if len(s) == 0 {
		return -1
	}
	sort.Ints(s)
	return s[0]
}

func (s sequences) Add(v int) sequences {
	return sequences(append(s, v))
}

func (s sequences) String() string {
	var str string
	for i, num := range s {
		if i == 0 {
			str = strconv.Itoa(num)
		} else {
			str += "," + strconv.Itoa(num)
		}
	}
	return str
}

type LogFn func(string, ...any)

func (fn LogFn) Log(f string, args ...any) {
	fn(f, args...)
}

func (fn LogFn) Printf(f string, args ...any) {
	fn(f, args...)
}

func log_prefix(fn LogFn, prefix string) LogFn {
	return func(f string, args ...any) {
		fn(prefix+f, args...)
	}
}

func fn_mutelog(string, ...any) {}

func STDErrLog(f string, args ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", args...)
}

func buildZnode(names ...string) string {
	return filepath.Join(names...)
}

func buildRoleEvent(role RoleType, node gen.Atom) fmt.Stringer {
	if role == Leader {
		return EventNodeSwitchedToLeader{Name: node}
	}
	return EventNodeSwitchedToFollower{Name: node}
}
