package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/Axiomatic314/qCIFF/ciff"
	"google.golang.org/protobuf/proto"
)

func ReadNextMessage(bufferedReader *bufio.Reader, messageStruct proto.Message) error {
	sizeBuffer, err := bufferedReader.Peek(binary.MaxVarintLen64)
	if err != nil {
		slog.Debug("error trying to peek at message length", "error", err)
	}
	messageSize, bytesRead := binary.Uvarint(sizeBuffer)
	bufferedReader.Discard(bytesRead)

	byteBuffer := make([]byte, messageSize)
	_, err = bufferedReader.Read(byteBuffer)
	if err != nil {
		slog.Debug("error reading message bytes", "error", err)
		return err
	}

	err = proto.Unmarshal(byteBuffer, messageStruct)
	if err != nil {
		slog.Debug("error during unmarshal", "error", err)
		return err
	}

	return nil
}

func main() {

	ciffFilePath := flag.String("ciffFilePath", "", "filepath of CIFF file to read in")
	flag.Parse()

	ciffFileHandle, err := os.Open(*ciffFilePath)
	if err != nil {
		slog.Error("error opening ciff", "error", err)
		os.Exit(1)
	}

	ciffReader := bufio.NewReader(ciffFileHandle)

	// --------------------------------------------------------------------------------
	// Header
	slog.Debug("reading header")

	header := &ciff.Header{}
	err = ReadNextMessage(ciffReader, header)
	if err != nil {
		slog.Error("error reading header message", "error", err)
		os.Exit(1)
	}
	fmt.Printf("Header: %+v\n", header)

	// --------------------------------------------------------------------------------
	// PostingsList
	slog.Debug("reading postings lists")

	postingsListSlice := make([]*ciff.PostingsList, header.NumPostingsLists)
	for postingsListIndex := range header.NumPostingsLists {
		postingsList := &ciff.PostingsList{}
		ReadNextMessage(ciffReader, postingsList)
		postingsListSlice[postingsListIndex] = postingsList
		slog.Debug("postingsList decoded", "index", postingsListIndex, "postingsList", postingsListSlice[postingsListIndex])
	}
	for postingsListIndex, postingsList := range postingsListSlice {
		fmt.Printf("Postings List %v: %+v\n", postingsListIndex, postingsList)
	}

	// --------------------------------------------------------------------------------
	// DocRecord
	slog.Debug("reading doc records")

	docRecordSlice := make([]*ciff.DocRecord, header.NumDocs)
	for docRecordIndex := range header.NumDocs {
		r := &ciff.DocRecord{}
		ReadNextMessage(ciffReader, r)
		docRecordSlice[docRecordIndex] = r
		slog.Debug("docRecord decoded", "index", docRecordIndex, "docRecord", docRecordSlice[docRecordIndex])
	}
	for docRecordIndex, docRecord := range docRecordSlice {
		fmt.Printf("Doc Record %v: %+v\n", docRecordIndex, docRecord)
	}
}
