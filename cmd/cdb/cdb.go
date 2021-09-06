// Copyright 2021 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/UNO-SOFT/mcdb"
	"github.com/peterbourgon/ff/v3/ffcli"
)

func main() {
	if err := Main(); err != nil {
		log.Fatalf("ERROR: %+v\n", err)
	}
}

func Main() error {
	dumpCmd := ffcli.Command{Name: "dump",
		Exec: func(ctx context.Context, args []string) error {
			cr, err := mcdb.NewReader(args[0])
			if err != nil {
				return err
			}
			defer cr.Close()
			return cr.Dump(os.Stdout)
		},
	}

	fs := flag.NewFlagSet("make", flag.ContinueOnError)
	flagMakeCount := fs.Int("tables", 2, "number of tables to create")
	makeCmd := ffcli.Command{Name: "make", FlagSet: fs,
		Exec: func(ctx context.Context, args []string) error {
			cw, err := mcdb.NewWriter(args[0], *flagMakeCount)
			if err != nil {
				return err
			}
			defer cw.Close()
			if err := cw.Load(os.Stdin); err != nil {
				return err
			}
			return cw.Close()
		},
	}

	app := ffcli.Command{Name: "cdb",
		Exec:        dumpCmd.Exec,
		Subcommands: []*ffcli.Command{&dumpCmd, &makeCmd},
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return app.ParseAndRun(ctx, os.Args[1:])
}
