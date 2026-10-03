// Command repl is an interactive playground for the cluster: issue client
// operations and inject failures by hand, then watch what Raft does.
//
//	go run ./cmd/repl          (3 nodes)
//	go run ./cmd/repl -n 5     (5 nodes)
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"raft-kv/kv"
	"raft-kv/raft"
)

const help = `commands:
  put <key> <value>      write through the Clerk (finds the leader, retries)
  get <key>              read through the Clerk
  del <key>              delete through the Clerk
  putat <id> <key> <v>   write directly at one node (shows what a stale leader does)
  getat <id> <key>       read directly at one node (dirty read on a stale leader)
  status                 role, term, log length and commit index of every node
  dump                   key/value contents of every node
  kill <id>              crash a node permanently (no persistence yet, so no restart)
  disconnect <id>        partition a node away from the others (it keeps running)
  reconnect <id>         heal the partition
  help | quit`

func main() {
	n := flag.Int("n", 3, "number of nodes")
	flag.Parse()

	nodes := make([]*raft.Raft, *n)
	servers := make([]*kv.KVServer, *n)
	health := make([]string, *n)

	for i := 0; i < *n; i++ {
		var peers []int
		for p := 0; p < *n; p++ {
			if p != i {
				peers = append(peers, p)
			}
		}
		applyCh := make(chan raft.ApplyMsg, 100)
		nodes[i] = raft.NewRaft(i, peers, applyCh)
		servers[i] = kv.NewKVServer(nodes[i], applyCh)
		health[i] = "up"
	}
	for i, a := range nodes {
		for j, b := range nodes {
			if i != j {
				a.AddPeer(j, b)
			}
		}
	}
	for _, nd := range nodes {
		go nd.RunElectionTimer()
	}

	ck := kv.NewClerk(servers)

	nodeArg := func(s string) (int, bool) {
		id, err := strconv.Atoi(s)
		if err != nil || id < 0 || id >= *n {
			fmt.Printf("node id must be 0..%d\n", *n-1)
			return 0, false
		}
		return id, true
	}

	fmt.Println(help)
	sc := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) > 0 {
			switch {
			case f[0] == "quit" || f[0] == "exit":
				return

			case f[0] == "help":
				fmt.Println(help)

			case f[0] == "put" && len(f) == 3:
				fmt.Println("->", ck.Put(f[1], f[2]))

			case f[0] == "get" && len(f) == 2:
				v, ok, err := ck.Get(f[1])
				fmt.Printf("-> %q found=%v err=%v\n", v, ok, err)

			case f[0] == "del" && len(f) == 2:
				fmt.Println("->", ck.Delete(f[1]))

			case f[0] == "putat" && len(f) == 4:
				if id, ok := nodeArg(f[1]); ok {
					fmt.Println("->", servers[id].Put(f[2], f[3]))
				}

			case f[0] == "getat" && len(f) == 3:
				if id, ok := nodeArg(f[1]); ok {
					v, found, err := servers[id].Get(f[2])
					fmt.Printf("-> %q found=%v err=%v\n", v, found, err)
				}

			case f[0] == "status":
				for i, nd := range nodes {
					term, isLeader := nd.GetState()
					role := "follower"
					if isLeader {
						role = "LEADER"
					}
					log, commit := nd.GetLogState()
					fmt.Printf("node %d  %-8s term=%-3d log=%-3d commit=%-3d [%s]\n",
						i, role, term, len(log)-1, commit, health[i])
				}

			case f[0] == "dump":
				for i, s := range servers {
					fmt.Printf("node %d  %v\n", i, s.Dump())
				}

			case f[0] == "kill" && len(f) == 2:
				if id, ok := nodeArg(f[1]); ok {
					nodes[id].Kill()
					health[id] = "dead"
				}

			case f[0] == "disconnect" && len(f) == 2:
				if id, ok := nodeArg(f[1]); ok && health[id] != "dead" {
					nodes[id].Disconnect()
					health[id] = "isolated"
				}

			case f[0] == "reconnect" && len(f) == 2:
				if id, ok := nodeArg(f[1]); ok && health[id] != "dead" {
					nodes[id].Reconnect()
					health[id] = "up"
				}

			default:
				fmt.Println("unrecognised command; type 'help'")
			}
		}
		fmt.Print("> ")
	}
}