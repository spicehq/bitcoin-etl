package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/apache/arrow/go/v8/arrow/ipc"
)

func main() {
	python := "/opt/homebrew/Caskroom/miniforge/base/envs/bitcoin-etl/bin/python3"

	args := []string{
		"bitcoinetl.py",
		"export_all",
		"--start",
		"700000",
		"--end",
		"700000",
		"--provider-uri",
		"http://bitcoin:bitcoin@127.0.0.1:8332",
		"--enrich",
		"true",
	}

	etlCmd := exec.Command(python, args...)

	reader, err := etlCmd.StdoutPipe()
	must(err)

	etlCmd.Stderr = os.Stdout

	etlCmd.Start()

	rdr, err := ipc.NewReader(reader)
	must(err)
	for rdr.Next() {
		r := rdr.Record()
		fmt.Printf("num_rows=%d\n", r.NumRows())
		fmt.Printf("schema=%+v\n", r.Schema())
	}

	etlCmd.Wait()
}

func must(err error) {
	if err != nil {
		panic(err.Error())
	}
}
