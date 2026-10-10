// Copyright 2021, 2024 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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

	"flag"
	"github.com/UNO-SOFT/cli"
	"github.com/UNO-SOFT/mcdb"
)

func main() {
	if err := Main(); err != nil {
		log.Fatalf("ERROR: %+v\n", err)
	}
}

func Main() error {
	var simple, onlyKeys bool

	FS := flag.NewFlagSet("dump", flag.ContinueOnError)
	FS.BoolVar(&onlyKeys, "list-only", false, "list only the keys")
	dumpCmd := cli.Command{Name: "dump", Flags: FS,
		Exec: func(ctx context.Context, state *cli.State) error {
			args := state.Args
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
		}, FlagConfigs: []cli.FlagConfig{cli.FlagConfig{Name: "list-only", Short: "l"}},
	}

	FS = flag.NewFlagSet("make", flag.ContinueOnError)
	flagMakeCount := FS.Int("tables", 1, "number of tables to create")
	makeCmd := cli.Command{Name: "make", Flags: FS,
		Exec: func(ctx context.Context, state *cli.State) error {
			args := state.Args
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
		}, FlagConfigs: []cli.FlagConfig{cli.FlagConfig{Name: "tables", Short: "t"}},
	}

	FS = flag.NewFlagSet("cdb", flag.ContinueOnError)
	FS.BoolVar(&simple, "simple", false, "simple format (key, whitespace, rest is value till EOL)")
	app := cli.Command{Name: "cdb", Flags: FS,
		Exec:        dumpCmd.Exec,
		SubCommands: []*cli.Command{&dumpCmd, &makeCmd}, FlagConfigs: []cli.FlagConfig{cli.FlagConfig{Name: "simple", Short: "m"}},
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := cli.Parse(&app, os.Args[1:]); err != nil {
		cli.PrintHelp(os.Stderr, &app)
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	return cli.Run(ctx, &app, nil)
}
