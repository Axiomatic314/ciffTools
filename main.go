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

// func CiffToHuman()

func main() {

	ciffFilePath := flag.String("ciffFilePath", "", "filepath of CIFF file to read in")
	dictFilePath := flag.String("dictFilePath", "", "filepath for dictionary")
	postingsFilePath := flag.String("postingsFilePath", "", "filepath for PostingsLists")
	docRecordsFilePath := flag.String("docRecordsFilePath", "", "filepath for docRecords")
	headerFilePath := flag.String("headerFilePath", "", "filepath for header")
	flag.Parse()

	ciffFileHandle, err := os.Open(*ciffFilePath)
	if err != nil {
		slog.Error("error opening ciff", "error", err)
		os.Exit(1)
	}
	defer ciffFileHandle.Close()

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
	//fmt.Printf("Header: %+v\n", header)

	// --------------------------------------------------------------------------------
	// PostingsList
	slog.Debug("reading postings lists")

	postingsListSlice := make([]*ciff.PostingsList, header.NumPostingsLists)
	for postingsListIndex := range header.NumPostingsLists {
		postingsList := &ciff.PostingsList{}
		ReadNextMessage(ciffReader, postingsList)
		postingsListSlice[postingsListIndex] = postingsList
		slog.Debug("postingsList decoded", "index", postingsListIndex, "postingsList", postingsListSlice[postingsListIndex])
		//update d-gaps to be actual docids
		postings := postingsListSlice[postingsListIndex].GetPostings()
		prev := postings[0].GetDocid()
		for postingsIndex := range postingsListSlice[postingsListIndex].GetDf() {
			if postingsIndex > 0 {
				postings[postingsIndex].Docid += prev
				prev = postings[postingsIndex].GetDocid()
			}
		}
	}

	for postingsListIndex, postingsList := range postingsListSlice {
		fmt.Printf("Postings List %v: %+v)\n", postingsListIndex, postingsList)
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
	// for docRecordIndex, docRecord := range docRecordSlice {
	// 	fmt.Printf("Doc Record %v: %+v\n", docRecordIndex, docRecord)
	// }

	//Header
	headerFileHandle, err := os.Create(*headerFilePath)
	if err != nil {
		slog.Error("error opening header", "error", err)
		os.Exit(1)
	}
	defer headerFileHandle.Close()
	headerWriter := bufio.NewWriter(headerFileHandle)
	headerWriter.WriteString(fmt.Sprintf("Version: %v\n", header.Version))
	headerWriter.WriteString(fmt.Sprintf("NumPostingsLists: %v\n", header.NumPostingsLists))
	headerWriter.WriteString(fmt.Sprintf("NumDocs: %v\n", header.NumDocs))
	headerWriter.WriteString(fmt.Sprintf("TotalPostingsLists: %v\n", header.TotalPostingsLists))
	headerWriter.WriteString(fmt.Sprintf("TotalDocs: %v\n", header.TotalDocs))
	headerWriter.WriteString(fmt.Sprintf("TotalTermsInCollection: %v\n", header.TotalTermsInCollection))
	headerWriter.WriteString(fmt.Sprintf("AverageDocLength: %v\n", header.AverageDoclength))
	headerWriter.WriteString(fmt.Sprintf("Description: %v\n", header.Description))
	headerWriter.Flush()

	//Dictionary
	dictFileHandle, err := os.Create(*dictFilePath)
	if err != nil {
		slog.Error("error opening dictionary", "error", err)
		os.Exit(1)
	}
	defer dictFileHandle.Close()
	dictWriter := bufio.NewWriter(dictFileHandle)
	for postingsListIndex := range postingsListSlice {
		dictWriter.WriteString(postingsListSlice[postingsListIndex].Term)
		dictWriter.WriteString("\n")
	}
	dictWriter.Flush()

	//PostingsLists
	postingsFileHandle, err := os.Create(*postingsFilePath)
	if err != nil {
		slog.Error("error opening postingsList", "error", err)
		os.Exit(1)
	}
	defer postingsFileHandle.Close()
	postingsWriter := bufio.NewWriter(postingsFileHandle)
	postingsWriter.WriteString("term df cf (docid, tf) ... (docid, tf)\n")
	postingsWriter.WriteString("--------------------------------------\n")
	for _, postingsList := range postingsListSlice {
		postingsWriter.WriteString(fmt.Sprintf("%s %d %d ", postingsList.Term, postingsList.Df, postingsList.Cf))
		for _, posting := range postingsList.GetPostings() {
			postingsWriter.WriteString(fmt.Sprintf("(%d, %d) ", posting.Docid, posting.Tf))
		}
		postingsWriter.WriteString("\n")
	}
	postingsWriter.Flush()

	//DocRecords
	docRecordsFileHandle, err := os.Create(*docRecordsFilePath)
	if err != nil {
		slog.Error("error opening docRecords", "error", err)
		os.Exit(1)
	}
	defer docRecordsFileHandle.Close()
	docRecordsWriter := bufio.NewWriter(docRecordsFileHandle)
	docRecordsWriter.WriteString("docid collection_docid doclength\n")
	docRecordsWriter.WriteString("--------------------------------\n")
	for _, docRecord := range docRecordSlice {
		docRecordsWriter.WriteString(fmt.Sprintf("%d %s %d\n", docRecord.Docid, docRecord.CollectionDocid, docRecord.Doclength))
	}
	docRecordsWriter.Flush()
}
