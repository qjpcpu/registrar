package zk

import (
	"fmt"
	"path/filepath"

	"github.com/qjpcpu/zk"
)

type zkConn interface {
	AddAuth(scheme string, auth []byte) error
	Exists(path string) (bool, *zk.Stat, error)
	Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error)
	Set(path string, data []byte) (*zk.Stat, error)
	Delete(path string, version int32) error
	Get(path string) ([]byte, *zk.Stat, error)
	GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error)
	Children(path string) ([]string, *zk.Stat, error)
	ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error)
	CreateProtectedEphemeralSequential(path string, data []byte, acl []zk.ACL) (string, error)
	Close()
}

type zkConnImpl struct {
	conn *zk.Conn
}

func (impl *zkConnImpl) AddAuth(scheme string, auth []byte) error {
	return impl.conn.AddAuth(scheme, auth)
}

func (impl *zkConnImpl) Exists(path string) (bool, *zk.Stat, error) {
	return impl.conn.Exists(path)
}
func (impl *zkConnImpl) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	return impl.conn.Create(path, data, flags, acl)
}

func (impl *zkConnImpl) Set(path string, data []byte) (*zk.Stat, error) {
	return impl.conn.Set(path, data, -1)
}

func (impl *zkConnImpl) Delete(path string, version int32) error {
	return impl.conn.Delete(path, version)
}

func (impl *zkConnImpl) Get(path string) ([]byte, *zk.Stat, error) {
	return impl.conn.Get(path)
}

func (impl *zkConnImpl) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	return impl.conn.GetW(path)
}

func (impl *zkConnImpl) Children(path string) ([]string, *zk.Stat, error) {
	return impl.conn.Children(path)
}

func (impl *zkConnImpl) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	return impl.conn.ChildrenW(path)
}

func (impl *zkConnImpl) CreateProtectedEphemeralSequential(path string, data []byte, acl []zk.ACL) (string, error) {
	return impl.conn.CreateProtectedEphemeralSequential(path, data, acl)
}

func (impl *zkConnImpl) Close() {
	impl.conn.Close()
}

func ensurePersistentNode(conn zkConn, dir string) error {
	if dir == "/" {
		return nil
	}
	exist, _, err := conn.Exists(dir)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}
	if err = ensurePersistentNode(conn, filepath.Dir(dir)); err != nil {
		return err
	}
	if _, err = conn.Create(dir, []byte{}, zk.FlagPersistent, zk.WorldACL(zk.PermAll)); err != nil {
		return err
	}
	return nil
}

func createEphemeralSequentialChildNode(conn zkConn, tag, dir string, data []byte) (string, error) {
	if err := ensurePersistentNode(conn, dir); err != nil {
		return "", err
	}
	prefix := fmt.Sprintf("%s/%s-", dir, tag)
	return conn.CreateProtectedEphemeralSequential(prefix, data, zk.WorldACL(zk.PermAll))
}

func childrenNotContains(conn zkConn, dirNode string, childNode string) bool {
	children, _, err := conn.Children(dirNode)
	return err != nil || !stringContains(children, childNode)
}

func multiOnEvent(handlers ...func(zk.Event)) func(zk.Event) {
	return func(e zk.Event) {
		for _, fn := range handlers {
			fn(e)
		}
	}
}
