package main

import (
	"context"
	"fmt"
	"os"

	"github.com/raftlite/pkg/client"
)

func runStatus(c *client.Client, asJSON bool) {
	resp, err := c.Status(context.Background())
	if err != nil {
		if asJSON {
			printJSON(map[string]any{"error": err.Error()})
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		os.Exit(1)
	}

	if asJSON {
		printJSON(resp)
		return
	}

	fmt.Printf("Node ID:      %s\n", resp.NodeId)
	fmt.Printf("Role:         %s\n", resp.Role)
	fmt.Printf("Current Term: %d\n", resp.CurrentTerm)
	fmt.Printf("Commit Index: %d\n", resp.CommitIndex)
	fmt.Printf("Last Applied: %d\n", resp.LastApplied)
	if resp.LeaderId != "" {
		fmt.Printf("Leader ID:    %s\n", resp.LeaderId)
	}
	fmt.Printf("Peers (%d):\n", len(resp.Peers))
	for _, p := range resp.Peers {
		statusStr := "HEALTHY"
		if !p.Reachable {
			statusStr = "UNREACHABLE"
		}
		fmt.Printf("  - %s (%s): matchIndex=%d, nextIndex=%d [%s]\n",
			p.PeerId, p.Address, p.MatchIndex, p.NextIndex, statusStr)
	}
}
