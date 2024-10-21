package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/Axiomatic314/ciffTools/ciff"
)

func CiffToHuman(ciffFilePath string, outputDirectory string) {
	//create folder for the human-readable files
	err := os.Mkdir(outputDirectory, 0777)
	if err != nil && !os.IsExist(err) {
		slog.Error("cannot create output directory", "error", err)
		os.Exit(1)
	}

	//prepare to read ciff
	ciffFileHandle, err := os.Open(ciffFilePath)
	if err != nil {
		slog.Error("error opening ciff", "error", err)
		os.Exit(1)
	}
	defer ciffFileHandle.Close()
	ciffReader := bufio.NewReader(ciffFileHandle)

	//process header
	slog.Info("processing header")
	headerFileHandle, err := os.Create(filepath.Join(outputDirectory, "output.header"))
	if err != nil {
		slog.Error("error creating header", "error", err)
		os.Exit(1)
	}
	defer headerFileHandle.Close()
	headerWriter := bufio.NewWriter(headerFileHandle)

	header := &ciff.Header{}
	err = ReadNextMessage(ciffReader, header)
	if err != nil {
		slog.Error("error reading header message", "error", err)
		os.Exit(1)
	}

	headerWriter.WriteString(fmt.Sprintf("Version: %v\n", header.Version))
	headerWriter.WriteString(fmt.Sprintf("NumPostingsLists: %v\n", header.NumPostingsLists))
	headerWriter.WriteString(fmt.Sprintf("NumDocs: %v\n", header.NumDocs))
	headerWriter.WriteString(fmt.Sprintf("TotalPostingsLists: %v\n", header.TotalPostingsLists))
	headerWriter.WriteString(fmt.Sprintf("TotalDocs: %v\n", header.TotalDocs))
	headerWriter.WriteString(fmt.Sprintf("TotalTermsInCollection: %v\n", header.TotalTermsInCollection))
	headerWriter.WriteString(fmt.Sprintf("AverageDocLength: %v\n", header.AverageDoclength))
	headerWriter.WriteString(fmt.Sprintf("Description: %v\n", header.Description))
	headerWriter.Flush()

	//process postings
	slog.Info("processing postings lists")

	postingsFileHandle, err := os.Create(filepath.Join(outputDirectory, "output.postings"))
	if err != nil {
		slog.Error("error opening postingsList", "error", err)
		os.Exit(1)
	}
	defer postingsFileHandle.Close()
	postingsWriter := bufio.NewWriter(postingsFileHandle)
	postingsWriter.WriteString("term df cf (docid, tf) ... (docid, tf)\n")
	postingsWriter.WriteString("--------------------------------------\n")

	postingsListDict := make([]string, header.NumPostingsLists) //to hold all terms

	n := header.NumPostingsLists / 10
	for postingsListIndex := range header.NumPostingsLists {
		if postingsListIndex%n == 0 {
			slog.Info(fmt.Sprintf("postings list %d/%d", postingsListIndex, header.NumPostingsLists))
		}
		postingsList := &ciff.PostingsList{}
		ReadNextMessage(ciffReader, postingsList)
		postingsWriter.WriteString(fmt.Sprintf("%s %d %d ", postingsList.Term, postingsList.Df, postingsList.Cf))
		postingsListDict[postingsListIndex] = postingsList.Term
		postings := postingsList.Postings
		currDocid := postings[0].Docid
		for postingsIndex := range postingsList.Df {
			if postingsIndex > 0 {
				currDocid += postings[postingsIndex].Docid
			}
			postingsWriter.WriteString(fmt.Sprintf("(%d, %d) ", currDocid, postings[postingsIndex].Tf))
		}
		postingsWriter.WriteString("\n")
	}
	postingsWriter.Flush()

	//write dictionary
	slog.Info("processing dictionary")

	dictFileHandle, err := os.Create(filepath.Join(outputDirectory, "output.dict"))
	if err != nil {
		slog.Error("error creating dictionary", "error", err)
		os.Exit(1)
	}
	defer dictFileHandle.Close()
	dictWriter := bufio.NewWriter(dictFileHandle)

	for _, term := range postingsListDict {
		dictWriter.WriteString(term)
		dictWriter.WriteString("\n")
	}
	dictWriter.Flush()

	//process docRecords
	slog.Info("processing docRecords")
	docRecordsFileHandle, err := os.Create(filepath.Join(outputDirectory, "output.docRecords"))
	if err != nil {
		slog.Error("error opening docRecords", "error", err)
		os.Exit(1)
	}
	defer docRecordsFileHandle.Close()
	docRecordsWriter := bufio.NewWriter(docRecordsFileHandle)

	docRecordsWriter.WriteString("docid collection_docid doclength\n")
	docRecordsWriter.WriteString("--------------------------------\n")

	for range header.NumDocs {
		docRecord := &ciff.DocRecord{}
		ReadNextMessage(ciffReader, docRecord)
		docRecordsWriter.WriteString(fmt.Sprintf("%d %s %d\n", docRecord.Docid, docRecord.CollectionDocid, docRecord.Doclength))
	}
	docRecordsWriter.Flush()

}
