package internal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func Transfer(r io.Reader, w io.Writer, args []string) error {
	lfsPath := strings.Replace(strings.Replace(args[1], "'/", "", -1), "'", "", -1) + "/.git/lfs"

	if _, err := os.Stat(lfsPath); os.IsNotExist(err) {
		err := os.MkdirAll(lfsPath, os.ModePerm) // .git/lfs
		if err != nil {
			return err
		}
	}

	lfsDirs := []string{"objects", "incomplete", "tmp", "locks"}
	for _, lfsDir := range lfsDirs {
		if _, err := os.Stat(filepath.Join(lfsPath, lfsDir)); os.IsNotExist(err) {
			err := os.MkdirAll(lfsPath+"/"+lfsDir, os.ModePerm)
			if err != nil {
				return err
			}
		}
	}

	cmd := args[2]
	c := NewPktlineChannel(r, w, lfsPath)
	c.fs.c = c
	err := c.Start()
	if err != nil {
		return err
	}
	for c.Scan() {
		if c.req.err == io.EOF {
			// nothing to read - EOF channel
			break
		}
		if len(c.req.args) == 0 {
			// nothing to read, args empty
			continue
		}
		if len(c.req.args) > 2 {
			if strings.HasPrefix(c.req.args[2], "hash-algo=") {
				if c.req.args[2] != "hash-algo=sha256" {
					return c.SendMessage([]string{"status 400"}, []string{"unsupported hash algorithm"})
				}
			}
		}
		if c.req.args[0] == "quit" {
			err = c.End()
			if err != nil {
				return err
			}
			break
		}
		if c.req.args[0] == "version 1" {
			err = c.SendMessage([]string{"status 200"}, nil)
			if err != nil {
				return err
			}
		}
		if c.req.args[0] == "list-lock" || c.req.args[0] == "list-locks" {
			msgs, err := c.fs.listLocks()
			if err != nil {
				return c.SendMessage([]string{"status 400"}, []string{fmt.Sprintf("%s", err)})
			}
			err = c.SendMessage([]string{"status 202"}, msgs)
			if err != nil {
				return err
			}
		}
		if c.req.args[0] == "batch" {
			files, err := c.fs.batchObjects(cmd)
			if err != nil {
				return c.SendMessage([]string{"status 400"}, []string{fmt.Sprintf("%s", err)})
			}
			err = c.SendMessage([]string{"status 200", "hash-algo=sha256"}, files)
			if err != nil {
				return err
			}
		}
		if strings.HasPrefix(c.req.args[0], "verify-object") {
			err = c.fs.verifyObject()
			if err != nil {
				return c.SendMessage([]string{"status 400"}, []string{fmt.Sprintf("%s", err)})
			} else {
				err = c.SendMessage([]string{"status 200"}, nil)
				if err != nil {
					continue
				}
			}
		}
		if strings.HasPrefix(c.req.args[0], "get-object") {
			errc := make(chan error, 1)

			go func() {
				errc <- c.fs.getObject()
			}()

			err := <-errc
			if err != nil {
				err = c.SendMessage([]string{"status 400"}, []string{fmt.Sprintf("%s", err)})
				if err != nil {
					return err
				}
			}
		}
		if strings.HasPrefix(c.req.args[0], "put-object") {
			err = c.fs.storeObject()
			if err != nil {
				return c.SendMessage([]string{"status 400"}, []string{fmt.Sprintf("%s", err)})
			} else {
				err = c.SendMessage([]string{"status 200"}, nil)
				if err != nil {
					return err
				}
			}
		}
		if strings.HasPrefix(c.req.args[0], "lock") {
			msgs, err := c.fs.lockObject()
			if err != nil {
				return c.SendMessage([]string{"status 409"}, append(msgs, []string{fmt.Sprintf("%s", err)}...))
			} else {
				err = c.SendMessage(append([]string{"status 201"}, msgs...), nil)
				if err != nil {
					return err
				}
			}
		}
		if strings.HasPrefix(c.req.args[0], "unlock") {
			msgs, err := c.fs.unlockObject()
			if err != nil {
				return c.SendMessage([]string{"status 400"}, []string{fmt.Sprintf("%s", err)})
			} else {
				err = c.SendMessage(append([]string{"status 200"}, msgs...), nil)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
