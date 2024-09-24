package main

import (
	"fmt"
	"os"

	"github.com/Axiomatic314/qCIFF/ciff"
	"google.golang.org/protobuf/proto"
)

func main() {
	filename := "test-complete.ciff"
	in, _ := os.ReadFile(filename)
	header := &ciff.Header{}
	// n, t := protowire.ConsumeTag(int)
	// fmt.Printf("%d, %d\n", n, t)
	if err := proto.Unmarshal(in, header); err != nil {
		fmt.Println("Failed to parse header:", err)
	}
}
