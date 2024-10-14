package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/Axiomatic314/qCIFF/ciff"
	"github.com/Axiomatic314/qCIFF/quantize"
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
	// bytesRead, err = bufferedReader.Read(byteBuffer)
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
	ciffFileHandle, err := os.Create(writer.ciffFilePath)
	if err != nil {
		slog.Error("error writing ciff", "error", err)
		return err
	}
	defer ciffFileHandle.Close()

	ciffWriter := bufio.NewWriter(ciffFileHandle)

	//Header
	slog.Debug("writing header")

	err = WriteNextMessage(ciffWriter, header)
	if err != nil {
		slog.Error("error writing header message", "error", err)
		return err
	}

	//Postings
	slog.Debug("writing postings lists")

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
	slog.Debug("writing doc records")

	for docRecordIndex := range docRecords {
		docRecord := docRecords[docRecordIndex]
		WriteNextMessage(ciffWriter, docRecord)
		slog.Debug("doc record written", "index", docRecordIndex)
	}

	return nil
}

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	ciffFilePath := flag.String("ciffFilePath", "", "filepath of CIFF file to read in")
	writeHeader := flag.Bool("writeHeader", false, "Bool to write header file. Defaults to false.")
	writeDict := flag.Bool("writeDict", false, "Bool to write dictionary file. Defaults to false.")
	writePostings := flag.Bool("writePostings", false, "Bool to write postings file. Defaults to false.")
	writeDocRecords := flag.Bool("writeDocRecords", false, "Bool to write docRecords file. Defaults to false.")
	outputDirectory := flag.String("outputDirectory", "output", "The target output directory. If not already present, it is created relative to the current working directory. Any existing files are overwritten!")
	writeCiff := flag.Bool("writeCiff", false, "Bool to write ciff. Defaults to false.")
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

	outputCiffWriter := CiffWriter{
		writeCiff:    *writeCiff,
		ciffFilePath: fmt.Sprintf(*outputDirectory + "/q-" + *ciffFilePath),
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
		slog.Debug("postingsList", "term", postingsList.Term, "docFreq", postingsList.Df, "postingLen", len(postingsList.Postings))
		postings := postingsListSlice[postingsListIndex].Postings
		if len(postings) <= 0 {
			continue
		}
		prev := postings[0].Docid
		if postingsList.Df != int64(len(postings)) {
			slog.Error("Unexpected number of postings.", "DocFreq", postingsList.Df, "NumPostings", len(postings))
			// slog.Debug("", "size", proto.Size(postingsList), "reader", ciffReader.Size())
			os.Exit(1)
		}
		for postingsIndex := range postingsListSlice[postingsListIndex].Df {
			if postingsIndex > 0 {
				postings[postingsIndex].Docid += prev
				prev = postings[postingsIndex].Docid
			}
		}
		// slog.Debug("postingsList docids converted from d-gaps", "index", postingsListIndex)
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

	// --------------------------------------------------------------------------------
	// Quantize Index
	if *writeCiff {
		quantize.QuantizeIndex(postingsListSlice, docRecordSlice, header.AverageDoclength, header.NumDocs, 8)
	}

	// --------------------------------------------------------------------------------
	// Write output files
	err = os.Mkdir(*outputDirectory, 0777)
	if err != nil && !os.IsExist(err) {
		slog.Error("cannot create output directory", "error", err)
		os.Exit(1)
	}
	outputFileWriter.CiffToHuman(header, postingsListSlice, docRecordSlice)
	outputCiffWriter.WriteCiff(header, postingsListSlice, docRecordSlice)

}
