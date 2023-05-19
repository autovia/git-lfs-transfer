package internal

import (
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/git-lfs/pktline"
)

type PktlineChannel struct {
	mu   sync.Mutex
	pl   *pktline.Pktline
	req  *ChannelRequest
	fs   *Filesystem
	path string
	err  error
}

type ChannelRequest struct {
	args  []string
	lines []string
	data  io.Reader
	err   error
}

func NewPktlineChannel(r io.Reader, w io.Writer, p string) *PktlineChannel {
	pc := &PktlineChannel{
		pl:   pktline.NewPktline(r, w),
		path: p,
	}
	fs := &Filesystem{pc}
	pc.fs = fs
	return pc
}

func (pc *PktlineChannel) Lock() {
	pc.mu.Lock()
}

func (pc *PktlineChannel) Unlock() {
	pc.mu.Unlock()
}

func (pc *PktlineChannel) Start() error {
	pc.Lock()
	defer pc.Unlock()
	err := pc.SendMessage([]string{"version=1", "locking"}, nil)
	if err != nil {
		return err
	}
	return nil
}

func (pc *PktlineChannel) End() error {
	pc.Lock()
	defer pc.Unlock()
	err := pc.SendMessage([]string{"status 200"}, nil)
	if err != nil {
		return err
	}
	return nil
}

func (pc *PktlineChannel) Scan() bool {
	pc.req, pc.err = nil, nil
	req, err := pc.readRequest()
	if err != nil {
		pc.err = err
		return false
	}
	pc.req = req
	return true
}

func (pc *PktlineChannel) SendMessageData(args []string, data io.Reader) error {
	for _, arg := range args {
		err := pc.pl.WritePacketText(arg)
		if err != nil {
			return err
		}
	}
	err := pc.pl.WriteDelim()
	if err != nil {
		return err
	}
	buf := make([]byte, 32768)
	for {
		n, err := data.Read(buf)
		if n > 0 {
			err := pc.pl.WritePacket(buf[0:n])
			if err != nil {
				return err
			}
		}
		if err != nil {
			break
		}
	}
	return pc.pl.WriteFlush()
}

func (pc *PktlineChannel) SendMessage(args []string, lines []string) error {
	if len(args) > 0 {
		for _, arg := range args {
			err := pc.pl.WritePacketText(arg)
			if err != nil {
				return err
			}
		}
	}

	if len(lines) > 0 {
		err := pc.pl.WriteDelim()
		if err != nil {
			return err
		}
		for _, line := range lines {
			err = pc.pl.WritePacketText(line)

			if err != nil {
				return err
			}
		}
	}

	return pc.pl.WriteFlush()
}

func (pc *PktlineChannel) ReadMessage() ([]string, []string, io.Reader, error) {
	args := make([]string, 0, 100)
	lines := make([]string, 0, 100)
	delim := false
	data := false
	for {
		s, pktLen, err := pc.pl.ReadPacketTextWithLength()
		if err != nil {
			return nil, nil, nil, err
		}
		if strings.HasPrefix(s, "put-object") {
			data = true
		}
		if data {
			if pktLen == 0 {
				return nil, nil, nil, fmt.Errorf("unexpected flush packet")
			} else if pktLen == 1 {
				break
			} else {
				args = append(args, s)
			}
		} else {
			switch {
			case pktLen == 0:
				return args, lines, nil, nil
			case delim:
				lines = append(lines, s)
			case pktLen == 1:
				if delim {
					return nil, nil, nil, fmt.Errorf("unexpected delimiter packet")
				}
				delim = true
			default:
				args = append(args, s)
			}
		}
	}
	if data {
		return args, nil, pktline.NewPktlineReaderFromPktline(pc.pl, 65536), nil
	}
	return args, lines, nil, nil
}

func (pc *PktlineChannel) readRequest() (*ChannelRequest, error) {
	args, lines, data, err := pc.ReadMessage()
	req := &ChannelRequest{
		args:  args,
		lines: lines,
		data:  data,
		err:   err,
	}
	return req, nil
}
