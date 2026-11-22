package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/raftlite/pkg/client"
)

func main() {
	fs := flag.NewFlagSet("raftctl", flag.ExitOnError)
	var (
		flagEndpoints = fs.String("endpoints", "127.0.0.1:8081", "Comma-separated list of cluster endpoints")
		flagTimeout   = fs.Duration("timeout", 3*time.Second, "Request timeout duration")
		flagToken     = fs.String("token", "", "Cluster authorization token")
		flagJSON      = fs.Bool("json", false, "Output results in JSON format")
	)

	// Custom flag parsing to allow global flags before or after subcommands
	// Parse until first non-flag argument
	args := os.Args[1:]
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	// Look for global flags before subcommand
	parsedArgs := make([]string, 0)
	var subcommand string
	var subArgs []string

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			parsedArgs = append(parsedArgs, arg)
			// Check if next arg is value for this flag
			if (arg == "--endpoints" || arg == "-endpoints" ||
				arg == "--timeout" || arg == "-timeout" ||
				arg == "--token" || arg == "-token") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
				parsedArgs = append(parsedArgs, args[i])
			}
		} else {
			subcommand = arg
			subArgs = args[i+1:]
			break
		}
	}

	_ = fs.Parse(parsedArgs)

	if subcommand == "" {
		printUsage()
		os.Exit(1)
	}

	eps := strings.Split(*flagEndpoints, ",")
	for i := range eps {
		eps[i] = strings.TrimSpace(eps[i])
	}

	if subcommand == "bench" {
		runBench(eps, *flagToken, subArgs)
		return
	}

	c := client.NewClient(client.Config{
		Endpoints: eps,
		Token:     *flagToken,
		Timeout:   *flagTimeout,
	})
	defer c.Close()

	switch subcommand {
	case "put":
		runPut(c, subArgs, *flagJSON)
	case "get":
		runGet(c, subArgs, *flagJSON)
	case "cas":
		runCAS(c, subArgs, *flagJSON)
	case "delete":
		runDelete(c, subArgs, *flagJSON)
	case "status":
		runStatus(c, *flagJSON)
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage of raftctl:
  raftctl [global-flags] <command> [command-args]

Global Flags:
  --endpoints string   Comma-separated cluster addresses (default "127.0.0.1:8081")
  --timeout duration   Request timeout (default 3s)
  --token string       Cluster authorization token
  --json               Output machine-readable JSON

Commands:
  put <key> <value>
  get <key>
  cas <key> <expected_value> <new_value>
  delete <key>
  status
  bench [bench-flags]`)
}
