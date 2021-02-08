package main

import (
	"kafka-agent/agent"
)

func main() {
	a, err := agent.New()
	if err != nil {
		panic(err)
	}
	err = a.Run()
	if err != nil {
		panic(err)
	}
}
