package internal

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
)

const testDir = "test"

// Tests based on https://github.com/bk2204/scutiger/tree/dev/scutiger-lfs

func TestUploadFile(t *testing.T) {

	input := `000eversion 1
0000000abatch
0011transfer=ssh
0015hash-algo=sha256
001crefname=refs/heads/main
000100476ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6
00000050put-object 6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090
000bsize=6
0001000aabc12300000053verify-object 6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090
000bsize=6
0000`

	expected := `000eversion=1
000clocking
0000000fstatus 200
0000000fstatus 200
0015hash-algo=sha256
0001004e6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6 upload
0000000fstatus 200
0000000fstatus 200
0000`

	result := new(bytes.Buffer)
	Transfer(bytes.NewReader([]byte(input)), result, []string{"", testDir, "upload"})
	if expected != result.String() {
		t.Errorf("result was incorrect\ngot: %s\n\nwant: %s", result, expected)
	}
	cleanup(t)
}

func TestUploadFiles(t *testing.T) {

	input := `000eversion 1
0000000abatch
0011transfer=ssh
0015hash-algo=sha256
001crefname=refs/heads/main
000100476ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6
0048995782686b231f9d20a09a10511f1c60d31cad546743331481b10453d684deee 32
00000050put-object 6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090
000bsize=6
0001000aabc12300000050put-object 995782686b231f9d20a09a10511f1c60d31cad546743331481b10453d684deee
000csize=32
00010024abc123abc123abc123abc123000000500000053verify-object 995782686b231f9d20a09a10511f1c60d31cad546743331481b10453d684deee
000bsize=32
00000053verify-object 6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090
000csize=6
0000`

	expected := `000eversion=1
000clocking
0000000fstatus 200
0000000fstatus 200
0015hash-algo=sha256
0001004e6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6 upload
004f995782686b231f9d20a09a10511f1c60d31cad546743331481b10453d684deee 32 upload
0000000fstatus 200
0000000fstatus 200
0000`

	result := new(bytes.Buffer)
	Transfer(bytes.NewReader([]byte(input)), result, []string{"", testDir, "upload"})
	if expected != result.String() {
		t.Errorf("result was incorrect\ngot: %s\n\nwant: %s", result, expected)
	}
	cleanup(t)
}

func TestSimpleUpload(t *testing.T) {

	input := "000eversion 1\n" +
		"0000000abatch\n" +
		"0011transfer=ssh\n" +
		"0015hash-algo=sha256\n" +
		"001crefname=refs/heads/main\n" +
		"000100476ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6\n" +
		"0048ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32\n" +
		"00000050put-object 6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090\n" +
		"000bsize=6\n" +
		"0001000aabc12300000050put-object ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626\n" +
		"000csize=32\n" +
		"00010024This is\x00a complicated\xc2\xa9message.\n" +
		"00000053verify-object 6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090\n" +
		"000bsize=6\n" +
		"00000053verify-object ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626\n" +
		"000csize=32\n" +
		"0000"

	expected := `000eversion=1
000clocking
0000000fstatus 200
0000000fstatus 200
0015hash-algo=sha256
0001004e6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6 upload
004fce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32 upload
0000000fstatus 200
0000000fstatus 200
0000000fstatus 200
0000000fstatus 200
0000`

	result := new(bytes.Buffer)
	Transfer(bytes.NewReader([]byte(input)), result, []string{"", testDir, "upload"})
	if expected != result.String() {
		t.Errorf("result was incorrect\ngot: %s\n\nwant: %s", result, expected)
	}
	cleanup(t)
}

func TestFailedVerify(t *testing.T) {

	input := "000eversion 1\n" +
		"0000000abatch\n" +
		"0011transfer=ssh\n" +
		"0015hash-algo=sha256\n" +
		"001crefname=refs/heads/main\n" +
		"000100476ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6\n" +
		"0048ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32\n" +
		"00000050put-object 6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090\n" +
		"000bsize=6\n" +
		"0001000aabc12300000050put-object ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626\n" +
		"000csize=32\n" +
		"00010024This is\x00a complicated\xc2\xa9message.\n" +
		"00000053verify-object 6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090\n" +
		"000bsize=5\n" +
		"00000053verify-object ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626\n" +
		"000csize=32\n" +
		"0000"

	expected := `000eversion=1
000clocking
0000000fstatus 200
0000000fstatus 200
0015hash-algo=sha256
0001004e6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6 upload
004fce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32 upload
0000000fstatus 200
0000000fstatus 200
0000000fstatus 400
0001002acan not verify file size after upload
0000`

	result := new(bytes.Buffer)
	Transfer(bytes.NewReader([]byte(input)), result, []string{"", testDir, "upload"})
	if expected != result.String() {
		t.Errorf("result was incorrect\ngot: %s\n\nwant: %s", result, expected)
	}
	cleanup(t)
}

func TestMissingObject(t *testing.T) {

	input := "000eversion 1\n" +
		"0000000abatch\n" +
		"0011transfer=ssh\n" +
		"0015hash-algo=sha256\n" +
		"001crefname=refs/heads/main\n" +
		"000100476ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6\n" +
		"0048ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32\n" +
		"00000050put-object 6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090\n" +
		"000bsize=6\n" +
		"0001000aabc12300000050put-object ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626\n" +
		"000csize=32\n" +
		"00010024This is\x00a complicated\xc2\xa9message.\n" +
		"00000053verify-object 0000000000000000000000000000000000000000000000000000000000000000\n" +
		"000bsize=5\n" +
		"00000053verify-object ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626\n" +
		"000csize=32\n" +
		"0000"

	expected := `000eversion=1
000clocking
0000000fstatus 200
0000000fstatus 200
0015hash-algo=sha256
0001004e6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6 upload
004fce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32 upload
0000000fstatus 200
0000000fstatus 200
0000000fstatus 400
0001000enot found
0000`

	result := new(bytes.Buffer)
	Transfer(bytes.NewReader([]byte(input)), result, []string{"", testDir, "upload"})
	if expected != result.String() {
		t.Errorf("result was incorrect\ngot: %s\n\nwant: %s", result, expected)
	}
	cleanup(t)
}

func TestInvalidHashAlgo(t *testing.T) {

	input := "000eversion 1\n" +
		"0000000abatch\n" +
		"0011transfer=ssh\n" +
		"0015hash-algo=sha512\n" +
		"001crefname=refs/heads/main\n" +
		"000100476ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6\n" +
		"0048ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32\n" +
		"0000"

	expected := `000eversion=1
000clocking
0000000fstatus 200
0000000fstatus 400
0001001funsupported hash algorithm
0000`

	result := new(bytes.Buffer)
	Transfer(bytes.NewReader([]byte(input)), result, []string{"", testDir, "upload"})
	if expected != result.String() {
		t.Errorf("result was incorrect\ngot: %s\n\nwant: %s", result, expected)
	}
	cleanup(t)
}

func TestSimpleDownload(t *testing.T) {

	inputUpload := "000eversion 1\n" +
		"0000000abatch\n" +
		"0011transfer=ssh\n" +
		"0015hash-algo=sha256\n" +
		"001crefname=refs/heads/main\n" +
		"000100476ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6\n" +
		"0048ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32\n" +
		"00000050put-object ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626\n" +
		"000csize=32\n" +
		"00010024This is\x00a complicated\xc2\xa9message.\n" +
		"00000053verify-object ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626\n" +
		"000csize=32\n" +
		"0000"

	expectedUpload := `000eversion=1
000clocking
0000000fstatus 200
0000000fstatus 200
0015hash-algo=sha256
0001004e6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6 upload
004fce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32 upload
0000000fstatus 200
0000000fstatus 200
0000`

	resultUpload := new(bytes.Buffer)
	Transfer(bytes.NewReader([]byte(inputUpload)), resultUpload, []string{"", testDir, "upload"})
	if expectedUpload != resultUpload.String() {
		t.Errorf("result was incorrect\ngot: %s\n\nwant: %s", resultUpload, expectedUpload)
	}

	inputDownload := "000eversion 1\n" +
		"0000000abatch\n" +
		"0011transfer=ssh\n" +
		"0015hash-algo=sha256\n" +
		"001crefname=refs/heads/main\n" +
		"000100476ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6\n" +
		"0048ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32\n" +
		"00000050get-object ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626\n" +
		"0000"

	expectedDownload := "000eversion=1\n" +
		"000clocking\n" +
		"0000000fstatus 200\n" +
		"0000000fstatus 200\n" +
		"0015hash-algo=sha256\n" +
		"0001004c6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6 noop\n" +
		"0051ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32 download\n" +
		"0000000fstatus 200\n" +
		"000csize=32\n" +
		"00010024This is\x00a complicated\xc2\xa9message.\n" +
		"0000"

	resultDownload := new(bytes.Buffer)
	Transfer(bytes.NewReader([]byte(inputDownload)), resultDownload, []string{"", testDir, "download"})
	if expectedDownload != resultDownload.String() {
		t.Errorf("result was incorrect\ngot: %s\n\nwant: %s", resultDownload, expectedDownload)
	}

	cleanup(t)
}

func TestInvalidUpload(t *testing.T) {

	inputUpload := "000eversion 1\n" +
		"0000000abatch\n" +
		"0011transfer=ssh\n" +
		"0015hash-algo=sha256\n" +
		"001crefname=refs/heads/main\n" +
		"000100476ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6\n" +
		"0048ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32\n" +
		"00000050put-object 6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090\n" +
		"000bsize=6\n" +
		"0001000aabc12300000050put-object ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626\n" +
		"000csize=32\n" +
		"00010024This is\x01a complicated\xc2\xa9message.\n" +
		"00000053verify-object 6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090\n" +
		"000bsize=6\n" +
		"00000053verify-object ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626\n" +
		"000csize=32\n" +
		"0000"

	expectedUpload := `000eversion=1
000clocking
0000000fstatus 200
0000000fstatus 200
0015hash-algo=sha256
0001004e6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6 upload
004fce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32 upload
0000000fstatus 200
0000000fstatus 400
000100afexpected OID ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626, got 367988c7cb91e13beda0a15fb271afcbf02fa7a0e75d9e25ac50b2b4b38af5f5 after 32 bytes written
0000`

	resultUpload := new(bytes.Buffer)
	Transfer(bytes.NewReader([]byte(inputUpload)), resultUpload, []string{"", testDir, "upload"})
	if expectedUpload != resultUpload.String() {
		t.Errorf("result was incorrect\ngot: %s\n\nwant: %s", resultUpload, expectedUpload)
	}

	cleanup(t)
}

func TestSimpleLocking(t *testing.T) {

	inputUpload := "000eversion 1\n" +
		"0000000abatch\n" +
		"0011transfer=ssh\n" +
		"0015hash-algo=sha256\n" +
		"001crefname=refs/heads/main\n" +
		"000100476ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6\n" +
		"0048ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32\n" +
		"00000050put-object ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626\n" +
		"000csize=32\n" +
		"00010024This is\x00a complicated\xc2\xa9message.\n" +
		"00000053verify-object ce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626\n" +
		"000csize=32\n" +
		"0000"

	expectedUpload := `000eversion=1
000clocking
0000000fstatus 200
0000000fstatus 200
0015hash-algo=sha256
0001004e6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 6 upload
004fce08b837fe0c499d48935175ddce784e8c372d3cfb1c574fe1caff605d4f0626 32 upload
0000000fstatus 200
0000000fstatus 200
0000`

	resultUpload := new(bytes.Buffer)
	Transfer(bytes.NewReader([]byte(inputUpload)), resultUpload, []string{"", testDir, "upload"})
	if expectedUpload != resultUpload.String() {
		t.Errorf("resultUpload was incorrect\ngot: %s\n\nwant: %s", resultUpload, expectedUpload)
	}

	inputLock := `000eversion 1
00000009lock
0012path=test.zip
001crefname=refs/heads/main
0000`

	resultLock := new(bytes.Buffer)
	Transfer(bytes.NewReader([]byte(inputLock)), resultLock, []string{"", testDir, "upload"})

	var locked string
	scanner := bufio.NewScanner(strings.NewReader(resultLock.String()))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "0023locked-at=") {
			locked = line[14:]
		}
	}

	expectedLock := fmt.Sprintf("000eversion=1\n"+
		"000clocking\n"+
		"0000000fstatus 200\n"+
		"0000000fstatus 200\n"+
		"0048id=c7b8de23fd238fe5e16f6f03b844022f9f72fd168a0704d82d58f19cf72b7aa3\n"+
		"0012path=test.zip\n"+
		"0023locked-at=%s\n"+
		"0012ownername=jan\n"+
		"0000", locked)

	if expectedLock != resultLock.String() {
		t.Errorf("resultLock was incorrect\ngot: %s\n\nwant: %s", resultLock, expectedLock)
	}

	inputLocks := `000eversion 1
0000000elist-lock
0012path=test.zip
001crefname=refs/heads/main
0000`

	expectedLocks := fmt.Sprintf("000eversion=1\n"+
		"000clocking\n"+
		"0000000fstatus 200\n"+
		"0000000fstatus 202\n"+
		"0001004alock c7b8de23fd238fe5e16f6f03b844022f9f72fd168a0704d82d58f19cf72b7aa3\n"+
		"0053path c7b8de23fd238fe5e16f6f03b844022f9f72fd168a0704d82d58f19cf72b7aa3 test.zip\n"+
		"0064locked-at c7b8de23fd238fe5e16f6f03b844022f9f72fd168a0704d82d58f19cf72b7aa3 %s\n"+
		"0053ownername c7b8de23fd238fe5e16f6f03b844022f9f72fd168a0704d82d58f19cf72b7aa3 jan\n"+
		"0050owner c7b8de23fd238fe5e16f6f03b844022f9f72fd168a0704d82d58f19cf72b7aa3 ours\n"+
		"0000", locked)

	resultLocks := new(bytes.Buffer)
	Transfer(bytes.NewReader([]byte(inputLocks)), resultLocks, []string{"", testDir, "upload"})
	if expectedLocks != resultLocks.String() {
		t.Errorf("resultLocks was incorrect\ngot: %s\n\nwant: %s", resultLocks, expectedLocks)
	}

	inputUnlock := `000eversion 1
0000000elist-lock
0012path=test.zip
0000000eversion 1
0000004cunlock c7b8de23fd238fe5e16f6f03b844022f9f72fd168a0704d82d58f19cf72b7aa3
0000`

	expectedUnlock := fmt.Sprintf("000eversion=1\n"+
		"000clocking\n"+
		"0000000fstatus 200\n"+
		"0000000fstatus 202\n"+
		"0001004alock c7b8de23fd238fe5e16f6f03b844022f9f72fd168a0704d82d58f19cf72b7aa3\n"+
		"0053path c7b8de23fd238fe5e16f6f03b844022f9f72fd168a0704d82d58f19cf72b7aa3 test.zip\n"+
		"0064locked-at c7b8de23fd238fe5e16f6f03b844022f9f72fd168a0704d82d58f19cf72b7aa3 %s\n"+
		"0053ownername c7b8de23fd238fe5e16f6f03b844022f9f72fd168a0704d82d58f19cf72b7aa3 jan\n"+
		"0050owner c7b8de23fd238fe5e16f6f03b844022f9f72fd168a0704d82d58f19cf72b7aa3 ours\n"+
		"0000000fstatus 200\n"+
		"0000000fstatus 200\n"+
		"0048id=c7b8de23fd238fe5e16f6f03b844022f9f72fd168a0704d82d58f19cf72b7aa3\n"+
		"000apath=\n"+
		"0023locked-at=%s\n"+
		"0012ownername=jan\n"+
		"0000", locked, locked)

	resultUnlock := new(bytes.Buffer)
	Transfer(bytes.NewReader([]byte(inputUnlock)), resultUnlock, []string{"", testDir, "upload"})
	if expectedUnlock != resultUnlock.String() {
		t.Errorf("resultUnlock was incorrect\ngot: %s\n\nwant: %s", resultUnlock, expectedUnlock)
	}

	cleanup(t)
}

func cleanup(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(testDir); !os.IsNotExist(err) {
		err := os.RemoveAll(testDir)
		if err != nil {
			log.Printf("can not delete test dir %s", testDir)
		}
	}
}
