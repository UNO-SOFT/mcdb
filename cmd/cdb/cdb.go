// Copyright 2021, 2024 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/UNO-SOFT/mcdb"
	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"
)

func main() {
	if err := Main(); err != nil {
		log.Fatalf("ERROR: %+v\n", err)
	}
}

func Main() error {
	var simple, onlyKeys bool

	FS := ff.NewFlagSet("dump")
	FS.BoolVar(&onlyKeys, 'l', "list-only", "list only the keys")
	dumpCmd := ff.Command{Name: "dump", Flags: FS,
		Exec: func(ctx context.Context, args []string) error {
			cr, err := mcdb.NewReader(args[0])
			if err != nil {
				return err
			}
			defer cr.Close()
			cr.Config.Simple, cr.Config.OnlyKeys = simple, onlyKeys
			if len(args) == 1 {
				return cr.DumpContext(ctx, os.Stdout)
			}

			bw := bufio.NewWriter(os.Stdout)
			defer bw.Flush()
			for _, k := range args[1:] {
				key := []byte(k)
				val, err := cr.Get(key)
				if err != nil {
					return fmt.Errorf("%q: %w", k, err)
				}
				if err = mcdb.Dump(bw, key, val); err != nil {
					return err
				}
			}
			return nil
		},
	}

	FS = ff.NewFlagSet("make")
	flagMakeCount := FS.Int('t', "tables", 1, "number of tables to create")
	makeCmd := ff.Command{Name: "make", Flags: FS,
		Exec: func(ctx context.Context, args []string) error {
			cw, err := mcdb.NewWriter(args[0], *flagMakeCount)
			if err != nil {
				return err
			}
			defer cw.Close()
			cw.Config.Simple, cw.Config.OnlyKeys = simple, onlyKeys
			if err := cw.LoadContext(ctx, os.Stdin); err != nil {
				return err
			}
			return cw.Close()
		},
	}

	FS = ff.NewFlagSet("cdb")
	FS.BoolVar(&simple, 'm', "simple", "simple format (key, whitespace, rest is value till EOL)")
	app := ff.Command{Name: "cdb", Flags: FS,
		Exec:        dumpCmd.Exec,
		Subcommands: []*ff.Command{&dumpCmd, &makeCmd},
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := app.Parse(os.Args[1:]); err != nil {
		ffhelp.Command(&app).WriteTo(os.Stderr)
		if errors.Is(err, ff.ErrHelp) {
			return nil
		}
		return err
	}
	return app.Run(ctx)
}
