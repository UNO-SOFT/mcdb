// Copyright 2021, 2022 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
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
	var simple, onlyKeys bool
	opts := make([]mcdb.Option, 0, 2)

	fs := flag.NewFlagSet("dump", flag.ContinueOnError)
	fs.BoolVar(&onlyKeys, "l", false, "list only the keys")
	dumpCmd := ffcli.Command{Name: "dump", FlagSet: fs,
		Exec: func(ctx context.Context, args []string) error {
			cr, err := mcdb.NewReader(args[0])
			if err != nil {
				return err
			}
			defer cr.Close()
			if len(args) == 1 {
				return cr.DumpContext(ctx, os.Stdout, opts...)
			}

			bw := bufio.NewWriter(os.Stdout)
			defer bw.Flush()
			for _, k := range args[1:] {
				key := []byte(k)
				val, err := cr.Get(key)
				if err != nil {
					return fmt.Errorf("%q: %w", k, err)
				}
				if err = mcdb.Dump(bw, key, val, opts...); err != nil {
					return err
				}
			}
			return nil
		},
	}

	fs = flag.NewFlagSet("make", flag.ContinueOnError)
	flagMakeCount := fs.Int("tables", 1, "number of tables to create")
	makeCmd := ffcli.Command{Name: "make", FlagSet: fs,
		Exec: func(ctx context.Context, args []string) error {
			cw, err := mcdb.NewWriter(args[0], *flagMakeCount)
			if err != nil {
				return err
			}
			defer cw.Close()
			if err := cw.LoadContext(ctx, os.Stdin, opts...); err != nil {
				return err
			}
			return cw.Close()
		},
	}

	fs = flag.NewFlagSet("cdb", flag.ContinueOnError)
	fs.BoolVar(&simple, "m", false, "simple format (key, whitespace, rest is value till EOL)")
	app := ffcli.Command{Name: "cdb",
		FlagSet:     fs,
		Exec:        dumpCmd.Exec,
		Subcommands: []*ffcli.Command{&dumpCmd, &makeCmd},
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := app.Parse(os.Args[1:]); err != nil {
		return err
	}
	if simple {
		opts = append(opts, mcdb.Simple)
	}
	if onlyKeys {
		opts = append(opts, mcdb.OnlyKeys)
	}
	return app.Run(ctx)
}
