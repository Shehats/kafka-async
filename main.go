package main

import (
	"os"

	"github.com/kafka-async/promise"
)

func main() {
	promise.From(nil)
	os.Exit(1)
}
