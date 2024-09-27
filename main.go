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

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

type FileWriter struct {
	writeHeader, writeDict, writePostings, writeDocRecords bool
	outputDirectory                                        string
}

func (writer FileWriter) CiffToHuman(header *ciff.Header, postingsLists []*ciff.PostingsList, docRecords []*ciff.DocRecord) {
	//Header
	if writer.writeHeader {
		headerFileHandle, err := os.Create(fmt.Sprintf("%v/output.header", writer.outputDirectory))
		if err != nil {
			slog.Error("error creating header", "error", err)
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
	}

	//Dictionary
	if writer.writeDict {
		dictFileHandle, err := os.Create(fmt.Sprintf("%v/output.dict", writer.outputDirectory))
		if err != nil {
			slog.Error("error creating dictionary", "error", err)
			os.Exit(1)
		}
		defer dictFileHandle.Close()
		dictWriter := bufio.NewWriter(dictFileHandle)
		for postingsListIndex := range postingsLists {
			dictWriter.WriteString(postingsLists[postingsListIndex].Term)
			dictWriter.WriteString("\n")
		}
		dictWriter.Flush()
	}

	//PostingsLists
	if writer.writePostings {
		postingsFileHandle, err := os.Create(fmt.Sprintf("%v/output.postings", writer.outputDirectory))
		if err != nil {
			slog.Error("error opening postingsList", "error", err)
			os.Exit(1)
		}
		defer postingsFileHandle.Close()
		postingsWriter := bufio.NewWriter(postingsFileHandle)
		postingsWriter.WriteString("term df cf (docid, tf) ... (docid, tf)\n")
		postingsWriter.WriteString("--------------------------------------\n")
		for _, postingsList := range postingsLists {
			postingsWriter.WriteString(fmt.Sprintf("%s %d %d ", postingsList.Term, postingsList.Df, postingsList.Cf))
			for _, posting := range postingsList.GetPostings() {
				postingsWriter.WriteString(fmt.Sprintf("(%d, %d) ", posting.Docid, posting.Tf))
			}
			postingsWriter.WriteString("\n")
		}
		postingsWriter.Flush()
	}

	//DocRecords
	if writer.writeDocRecords {
		docRecordsFileHandle, err := os.Create(fmt.Sprintf("%v/output.docRecords", writer.outputDirectory))
		if err != nil {
			slog.Error("error opening docRecords", "error", err)
			os.Exit(1)
		}
		defer docRecordsFileHandle.Close()
		docRecordsWriter := bufio.NewWriter(docRecordsFileHandle)
		docRecordsWriter.WriteString("docid collection_docid doclength\n")
		docRecordsWriter.WriteString("--------------------------------\n")
		for _, docRecord := range docRecords {
			docRecordsWriter.WriteString(fmt.Sprintf("%d %s %d\n", docRecord.Docid, docRecord.CollectionDocid, docRecord.Doclength))
		}
		docRecordsWriter.Flush()
	}
}

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
	writeHeader := flag.Bool("writeHeader", false, "Bool to write header file. Defaults to false.")
	writeDict := flag.Bool("writeDict", false, "Bool to write dictionary file. Defaults to false.")
	writePostings := flag.Bool("writePostings", false, "Bool to write postings file. Defaults to false.")
	writeDocRecords := flag.Bool("writeDocRecords", false, "Bool to write docRecords file. Defaults to false.")
	outputDirectory := flag.String("outputDirectory", "output", "The target output directory. If not already present, it is created relative to the current working directory. Any existing files are overwritten!")
	flag.Parse()

	if !isFlagPassed("ciffFilePath") {
		fmt.Println("Please provide a CIFF file!")
		os.Exit(1)
	}

	outputFileWriter := FileWriter{
		writeHeader:     *writeHeader,
		writeDict:       *writeDict,
		writePostings:   *writePostings,
		writeDocRecords: *writeDocRecords,
		outputDirectory: *outputDirectory,
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
	slog.Debug("reading header")

	header := &ciff.Header{}
	err = ReadNextMessage(ciffReader, header)
	if err != nil {
		slog.Error("error reading header message", "error", err)
		os.Exit(1)
	}

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

	err = os.Mkdir(*outputDirectory, 0777)
	if err != nil && !os.IsExist(err) {
		slog.Error("cannot create output directory", "error", err)
	}
	outputFileWriter.CiffToHuman(header, postingsListSlice, docRecordSlice)
}
