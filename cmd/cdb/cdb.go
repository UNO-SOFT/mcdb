// Copyright 2021 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"log"
	"os"

	"github.com/UNO-SOFT/mcdb"
)

func main() {
	if err := Main(); err != nil {
		log.Fatalf("ERROR: %+v\n", err)
	}
}

func Main() error {
	flag.Parse()
	cr, err := mcdb.NewReader(flag.Arg(0))
	if err != nil {
		return err
	}
	defer cr.Close()
	return cr.Dump(os.Stdout)
}
