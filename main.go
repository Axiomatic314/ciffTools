package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/Axiomatic314/ciffTools/ciff"
	"github.com/Axiomatic314/ciffTools/quantize"
	"google.golang.org/protobuf/proto"
)

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func ReadNextMessage(bufferedReader *bufio.Reader, messageStruct proto.Message) error {
	sizeBuffer, err := bufferedReader.Peek(binary.MaxVarintLen64)
	if err != nil {
		slog.Debug("error trying to peek at message length", "error", err)
	}
	messageSize, bytesRead := binary.Uvarint(sizeBuffer)
	bufferedReader.Discard(bytesRead)

	byteBuffer := make([]byte, messageSize)
	bytesRead, err = io.ReadFull(bufferedReader, byteBuffer)
	slog.Debug("reading message", "messageSize", messageSize, "bufferSize", binary.Size(byteBuffer), "bytesRead", bytesRead)
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

func WriteNextMessage(bufferedWriter *bufio.Writer, messageStruct proto.Message) error {
	byteBuffer, err := proto.Marshal(messageStruct)
	if err != nil {
		slog.Debug("error during marshal", "error", err)
		return err
	}

	sizeBuffer := make([]byte, binary.MaxVarintLen64)
	bytesWritten := binary.PutUvarint(sizeBuffer, uint64(len(byteBuffer)))

	_, err = bufferedWriter.Write(sizeBuffer[:bytesWritten])
	if err != nil {
		slog.Debug("error writing message size", "error", err)
		return err
	}

	_, err = bufferedWriter.Write(byteBuffer)
	if err != nil {
		slog.Debug("error writing message", "error", err)
		return err
	}

	bufferedWriter.Flush()

	return nil
}

type CiffWriter struct {
	writeCiff    bool
	ciffFilePath string
}

func (writer CiffWriter) WriteCiff(header *ciff.Header, postingsLists []*ciff.PostingsList, docRecords []*ciff.DocRecord) error {
	if !writer.writeCiff {
		return nil
	}
	slog.Info("writing ciff")
	ciffFileHandle, err := os.Create(writer.ciffFilePath)
	if err != nil {
		slog.Error("error writing ciff", "error", err)
		return err
	}
	defer ciffFileHandle.Close()
	ciffWriter := bufio.NewWriter(ciffFileHandle)

	//Header
	slog.Info("writing ciff header")
	err = WriteNextMessage(ciffWriter, header)
	if err != nil {
		slog.Error("error writing header message", "error", err)
		return err
	}

	//Postings
	slog.Info("writing ciff postings lists")
	for postingsListIndex := range header.NumPostingsLists {
		postingsList := postingsLists[postingsListIndex]
		//update docids to be d-gaps
		postings := postingsLists[postingsListIndex].GetPostings()
		prev := postings[0].GetDocid()
		for postingsIndex := range postingsLists[postingsListIndex].GetDf() {
			if postingsIndex > 0 {
				abs_docid := postings[postingsIndex].GetDocid()
				postings[postingsIndex].Docid = abs_docid - prev
				prev = abs_docid
			}
		}
		WriteNextMessage(ciffWriter, postingsList)
		slog.Debug("postingsList written", "index", postingsListIndex)
	}

	//DocRecords
	slog.Info("writing ciff doc records")
	for docRecordIndex := range docRecords {
		docRecord := docRecords[docRecordIndex]
		WriteNextMessage(ciffWriter, docRecord)
		slog.Debug("doc record written", "index", docRecordIndex)
	}

	return nil
}

func main() {
	// slog.SetLogLoggerLevel(slog.LevelDebug)

	ciffFilePath := flag.String("ciffFilePath", "", "filepath of CIFF file to read in")
	outputDirectory := flag.String("outputDirectory", "output", "The target output directory. If not already present, it is created relative to the current working directory. Any existing files are overwritten!")
	qciff := flag.Bool("qciff", false, "Bool to write quantized ciff. Defaults to false.")
	ciffToHuman := flag.Bool("ciffToHuman", false, "Bool to write human-readable dump of input ciff.")

	flag.Parse()

	if !isFlagPassed("ciffFilePath") {
		fmt.Println("Please provide a CIFF file!")
		os.Exit(1)
	}
	_, ciffFile := filepath.Split(*ciffFilePath)

	outputCiffWriter := CiffWriter{
		writeCiff:    *qciff,
		ciffFilePath: filepath.Join(*outputDirectory, fmt.Sprintf("q-%s", ciffFile)),
	}

	//for now, if ciffToHuman is called just close the program after
	if *ciffToHuman {
		slog.Info("creating human-readable ciff dump")
		CiffToHuman(*ciffFilePath, *outputDirectory)
		slog.Info("complete")
		os.Exit(0)
	}

	ciffFileHandle, err := os.Open(*ciffFilePath)
	if err != nil {
		slog.Error("error opening ciff", "error", err)
		os.Exit(1)
	}
	defer ciffFileHandle.Close()
	ciffReader := bufio.NewReader(ciffFileHandle)

	// --------------------------------------------------------------------------------
	// Header
	slog.Info("reading header")
	header := &ciff.Header{}
	err = ReadNextMessage(ciffReader, header)
	if err != nil {
		slog.Error("error reading header message", "error", err)
		os.Exit(1)
	}

	// --------------------------------------------------------------------------------
	// PostingsList
	slog.Info("reading postings lists")
	postingsListSlice := make([]*ciff.PostingsList, header.NumPostingsLists)
	n := header.NumPostingsLists / 10
	for postingsListIndex := range header.NumPostingsLists {
		if postingsListIndex%n == 0 {
			slog.Info(fmt.Sprintf("postings list %d/%d", postingsListIndex, header.NumPostingsLists))
		}
		postingsList := &ciff.PostingsList{}
		ReadNextMessage(ciffReader, postingsList)
		postingsListSlice[postingsListIndex] = postingsList
		slog.Debug("postingsList", "term", postingsList.Term, "docFreq", postingsList.Df, "postingLen", len(postingsList.Postings))
		postings := postingsListSlice[postingsListIndex].Postings
		if len(postings) <= 0 {
			continue
		}
		prev := postings[0].Docid
		if postingsList.Df != int64(len(postings)) {
			slog.Error("Unexpected number of postings.", "DocFreq", postingsList.Df, "NumPostings", len(postings))
			os.Exit(1)
		}
		for postingsIndex := range postingsListSlice[postingsListIndex].Df {
			if postingsIndex > 0 {
				postings[postingsIndex].Docid += prev
				prev = postings[postingsIndex].Docid
			}
		}
		slog.Debug("postingsList docids converted from d-gaps", "index", postingsListIndex)
	}

	// --------------------------------------------------------------------------------
	// DocRecord
	slog.Info("reading doc records")
	docRecordSlice := make([]*ciff.DocRecord, header.NumDocs)
	for docRecordIndex := range header.NumDocs {
		r := &ciff.DocRecord{}
		ReadNextMessage(ciffReader, r)
		docRecordSlice[docRecordIndex] = r
		slog.Debug("docRecord decoded", "index", docRecordIndex, "docRecord", docRecordSlice[docRecordIndex])
	}

	// --------------------------------------------------------------------------------
	// Quantize Index
	if *qciff {
		slog.Info("quantizing index")
		quantize.QuantizeIndex(postingsListSlice, docRecordSlice, header.AverageDoclength, header.NumDocs, 8)
	}

	// --------------------------------------------------------------------------------
	// Write output files
	err = os.Mkdir(*outputDirectory, 0777)
	if err != nil && !os.IsExist(err) {
		slog.Error("cannot create output directory", "error", err)
		os.Exit(1)
	}
	outputCiffWriter.WriteCiff(header, postingsListSlice, docRecordSlice)
	slog.Info("complete")
}
