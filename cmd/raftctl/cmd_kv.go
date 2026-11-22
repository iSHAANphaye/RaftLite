package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/raftlite/pkg/client"
)

func runPut(c *client.Client, args []string, asJSON bool) {
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: raftctl put <key> <value>\n")
		os.Exit(1)
	}
	key := args[0]
	val := args[1]

	resp, err := c.Put(context.Background(), key, val)
	if err != nil {
		if asJSON {
			printJSON(map[string]any{"success": false, "error": err.Error()})
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		os.Exit(1)
	}

	if asJSON {
		printJSON(map[string]any{
			"success": resp.Success,
			"key":     key,
			"value":   val,
			"error":   resp.Error,
		})
	} else {
		if resp.Success {
			fmt.Println("OK")
		} else {
			fmt.Printf("FAILED: %s\n", resp.Error)
		}
	}
}

func runGet(c *client.Client, args []string, asJSON bool) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: raftctl get <key>\n")
		os.Exit(1)
	}
	key := args[0]

	resp, err := c.Get(context.Background(), key, true)
	if err != nil {
		if asJSON {
			printJSON(map[string]any{"found": false, "error": err.Error()})
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		os.Exit(1)
	}

	if asJSON {
		printJSON(map[string]any{
			"key":   key,
			"found": resp.Found,
			"value": resp.Value,
			"error": resp.Error,
		})
	} else {
		if resp.Found {
			fmt.Println(resp.Value)
		} else {
			fmt.Println("KEY_NOT_FOUND")
		}
	}
}

func runCAS(c *client.Client, args []string, asJSON bool) {
	if len(args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: raftctl cas <key> <expected_value> <new_value>\n")
		os.Exit(1)
	}
	key := args[0]
	expected := args[1]
	newVal := args[2]

	resp, err := c.CAS(context.Background(), key, expected, newVal)
	if err != nil {
		if asJSON {
			printJSON(map[string]any{"swapped": false, "error": err.Error()})
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		os.Exit(1)
	}

	if asJSON {
		printJSON(map[string]any{
			"key":           key,
			"swapped":       resp.Swapped,
			"current_value": resp.CurrentValue,
			"error":         resp.Error,
		})
	} else {
		if resp.Swapped {
			fmt.Println("SWAPPED")
		} else {
			fmt.Printf("FAILED (current: %s)\n", resp.CurrentValue)
		}
	}
}

func runDelete(c *client.Client, args []string, asJSON bool) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: raftctl delete <key>\n")
		os.Exit(1)
	}
	key := args[0]

	resp, err := c.Delete(context.Background(), key)
	if err != nil {
		if asJSON {
			printJSON(map[string]any{"deleted": false, "error": err.Error()})
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		os.Exit(1)
	}

	if asJSON {
		printJSON(map[string]any{
			"key":     key,
			"found":   resp.Found,
			"success": resp.Success,
			"error":   resp.Error,
		})
	} else {
		if resp.Found {
			fmt.Println("DELETED")
		} else {
			fmt.Println("NOT_FOUND")
		}
	}
}

func printJSON(v any) {
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(data))
}
