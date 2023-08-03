package internal

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/git-lfs/git-lfs/v3/tools"
)

type Filesystem struct {
	c *PktlineChannel
}

func (fs *Filesystem) lockObject() ([]string, error) {
	var file string
	for _, arg := range fs.c.req.args {
		if strings.HasPrefix(arg, "path=") {
			file = arg[5:]
		}
	}

	hash := sha256.New()
	hash.Write([]byte(file))
	id := hex.EncodeToString(hash.Sum(nil))

	lockpath := filepath.Join(fs.c.path, "locks", id)
	if _, err := os.Stat(lockpath); err == nil {
		return nil, err
	} else {
		uid := syscall.Getuid()
		user, err := user.LookupId(strconv.Itoa(uid))
		if err != nil {
			return nil, err
		}

		now := time.Now().UTC().Format(time.RFC3339)

		lockfile, err := os.Create(lockpath)
		if err != nil {
			return nil, err
		}

		m := map[string]string{"path": file, "locked-at": now, "ownername": user.Username}
		b := new(bytes.Buffer)
		e := gob.NewEncoder(b)
		err = e.Encode(m)
		if err != nil {
			return nil, err
		}

		_, err = lockfile.Write(b.Bytes())
		if err != nil {
			return nil, err
		}

		lockfile.Close()

		msgs := []string{
			fmt.Sprintf("id=%s", id),
			fmt.Sprintf("path=%s", file),
			fmt.Sprintf("locked-at=%s", now),
			fmt.Sprintf("ownername=%s", user.Username),
		}

		return msgs, nil
	}
}

func (fs *Filesystem) listLocks() ([]string, error) {
	lockpath := filepath.Join(fs.c.path, "locks")

	files, err := os.ReadDir(lockpath)
	if err != nil {
		return nil, err
	}

	msgs := []string{}

	for _, file := range files {
		path := filepath.Join(lockpath, file.Name())
		decodedMap, err := readLockFile(path)
		if err != nil {
			return nil, err
		}

		msgs = append(msgs, fmt.Sprintf("lock %s", file.Name()))
		msgs = append(msgs, fmt.Sprintf("path %s %s", file.Name(), decodedMap["path"]))
		msgs = append(msgs, fmt.Sprintf("locked-at %s %s", file.Name(), decodedMap["locked-at"]))
		msgs = append(msgs, fmt.Sprintf("ownername %s %s", file.Name(), decodedMap["ownername"]))

		uid := syscall.Getuid()
		currentUser, err := user.LookupId(strconv.Itoa(uid))
		if err != nil {
			return nil, err
		}
		stat, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		fileinfo, ok := stat.Sys().(*syscall.Stat_t)
		if !ok {
			return nil, fmt.Errorf("cannot get user for file %q", path)
		}
		fileUser, err := user.LookupId(strconv.Itoa(int(fileinfo.Uid)))
		if err != nil {
			return nil, err
		}
		if currentUser.Username == fileUser.Username {
			msgs = append(msgs, fmt.Sprintf("owner %s %s", file.Name(), "ours"))
		} else {
			msgs = append(msgs, fmt.Sprintf("owner %s %s", file.Name(), "theirs"))
		}
	}

	return msgs, nil
}

func (fs *Filesystem) unlockObject() ([]string, error) {
	var id string
	var file string
	for _, arg := range fs.c.req.args {
		if strings.HasPrefix(arg, "path=") {
			file = arg[5:]
		}
	}

	values := strings.Split(fs.c.req.args[0], " ")
	if len(values) > 0 {
		id = values[1]
	}

	lockpath := filepath.Join(fs.c.path, "locks", id)
	if _, err := os.Stat(lockpath); err == nil {
		decodedMap, err := readLockFile(lockpath)
		if err != nil {
			return nil, err
		}

		msgs := []string{
			fmt.Sprintf("id=%s", id),
			fmt.Sprintf("path=%s", file),
			fmt.Sprintf("locked-at=%s", decodedMap["locked-at"]),
			fmt.Sprintf("ownername=%s", decodedMap["ownername"]),
		}

		err = os.Remove(lockpath)
		if err != nil {
			return nil, err
		}

		return msgs, nil

	} else {
		return nil, fmt.Errorf("lock does not exists")
	}
}

func (fs *Filesystem) getObject() error {
	var oid string
	var size int64
	var err error
	for _, arg := range fs.c.req.args {
		if strings.HasPrefix(arg, "get-object") {
			oid = strings.Split(arg, " ")[1]
		}
		fi, err := os.Stat(filepath.Join(fs.c.path, "objects", oid[0:2], oid[2:4], oid))
		if err != nil {
			return fmt.Errorf("not found")
		}
		size = fi.Size()
	}
	f, err := os.OpenFile(filepath.Join(fs.c.path, "objects", oid[0:2], oid[2:4], oid), os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	fs.c.pl.WritePacketText("status 200")
	fs.c.pl.WritePacketText(fmt.Sprintf("size=%v", size))
	fs.c.pl.WriteDelim()

	defer f.Close()
	type ProgressCallback func(totalSize, readSoFar int64, readSinceLast int) error
	var cb ProgressCallback
	ccb := func(totalSize int64, readSoFar int64, readSinceLast int) error {
		if cb != nil {
			return cb(totalSize, readSoFar, readSinceLast)
		}
		return nil
	}
	cbr := tools.NewFileBodyWithCallback(f, size, ccb)
	buf := make([]byte, 32768)
	for {
		n, err := cbr.Read(buf)
		if n > 0 {
			err := fs.c.pl.WritePacket(buf[0:n])
			if err != nil {
				return err
			}
		}
		if err != nil {
			break
		}
	}
	fs.c.pl.WriteFlush()
	return nil
}

func (fs *Filesystem) storeObject() error {
	var oid string
	var size int64
	var err error
	for _, arg := range fs.c.req.args {
		if strings.HasPrefix(arg, "put-object") {
			oid = strings.Split(arg, " ")[1]
		}
		if strings.HasPrefix(arg, "size=") {
			size, err = strconv.ParseInt(arg[5:], 10, 64)
			if err != nil {
				return err
			}
		}
	}
	var fa os.FileInfo
	fa, err = os.Stat(filepath.Join(fs.c.path, "objects", oid[0:2], oid[2:4], oid))
	if err == nil {
		if size == fa.Size() {
			// file already exists, nothing to do
			return nil
		}
	}

	dst, err := os.CreateTemp(filepath.Join(fs.c.path, "tmp"), "dst")
	if err != nil {
		return err
	}
	type ProgressCallback func(name string, totalSize, readSoFar int64, readSinceLast int) error
	var cb ProgressCallback
	ccb := func(totalSize int64, readSoFar int64, readSinceLast int) error {
		if cb != nil {
			return cb(oid, totalSize, readSoFar, readSinceLast)
		}
		return nil
	}

	hasher := tools.NewHashingReader(fs.c.req.data)
	written, err := tools.CopyWithCallback(dst, hasher, size, ccb)
	if err != nil {
		return fmt.Errorf("copyWithCallback %+v %+v, %+v, %s", dst, hasher, size, err)
	}
	if actual := hasher.Hash(); actual != oid {
		return fmt.Errorf("expected OID %s, got %s after %d bytes written", oid, actual, written)
	}
	if err := dst.Close(); err != nil {
		return err
	}
	fi, err := os.Stat(dst.Name())
	if err != nil {
		return fmt.Errorf("not found")
	}
	if size != fi.Size() {
		return fmt.Errorf("can not verify file size after upload")
	}
	err = os.MkdirAll(filepath.Join(fs.c.path, "objects", oid[0:2], oid[2:4]), os.ModePerm)
	if err != nil {
		return err
	}
	err = os.Rename(dst.Name(), filepath.Join(fs.c.path, "objects", oid[0:2], oid[2:4], oid))
	if err != nil {
		return err
	}
	err = os.Chmod(filepath.Join(fs.c.path, "objects", oid[0:2], oid[2:4], oid), 0775)
	if err != nil {
		return err
	}
	return nil
}

func (fs *Filesystem) verifyObject() error {
	var oid string
	var size int64
	var err error
	for _, arg := range fs.c.req.args {
		if strings.HasPrefix(arg, "verify-object") {
			oid = strings.Split(arg, " ")[1]
		}
		if strings.HasPrefix(arg, "size=") {
			size, err = strconv.ParseInt(arg[5:], 10, 64)
			if err != nil {
				return err
			}
		}
	}
	fi, err := os.Stat(filepath.Join(fs.c.path, "objects", oid[0:2], oid[2:4], oid))
	if err != nil {
		return fmt.Errorf("not found")
	}
	if size != fi.Size() {
		return fmt.Errorf("can not verify file size after upload")
	}
	return nil
}

func (fs *Filesystem) batchObjects(cmdIn string) ([]string, error) {
	files := []string{}
	for _, line := range fs.c.req.lines {
		cmdOut := cmdIn
		if cmdIn == "download" {
			oid := strings.Split(line, " ")[0]
			size, err := strconv.ParseInt(strings.Split(line, " ")[1], 10, 64)
			if err != nil {
				return nil, err
			}
			fi, err := os.Stat(filepath.Join(fs.c.path, "objects", oid[0:2], oid[2:4], oid))
			if err != nil {
				cmdOut = "noop"
			} else {
				if size != fi.Size() {
					cmdOut = "noop"
				}
			}
		}
		files = append(files, fmt.Sprintf("%s %s", line, cmdOut))
	}
	return files, nil
}

func readLockFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var decodedMap map[string]string
	d := gob.NewDecoder(f)
	err = d.Decode(&decodedMap)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return decodedMap, nil
}
