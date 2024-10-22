package quantize

import (
	"math"

	"github.com/Axiomatic314/ciffTools/ciff"
)

var smallestRSV float64 = math.MaxFloat64
var largestRSV float64 = 0

// uniform quantization
func quantize(x float64, bits int32) int32 {
	scale := math.Pow(2, float64(bits)) - 2
	return int32(((x-smallestRSV)/(largestRSV-smallestRSV))*scale) + 1
}

func QuantizeIndex(postingsLists []*ciff.PostingsList, docRecords []*ciff.DocRecord, averageDocLength float64, numDocs int32, bits int32, k1 float64, b float64) {
	scorer := rankingFunction{
		k1:               k1,
		b:                b,
		numDocs:          numDocs,
		averageDocLength: averageDocLength,
	}

	//find smallest and largest impacts
	for postingListIndex := range len(postingsLists) {
		idf := scorer.IDF(postingsLists[postingListIndex].Df)
		postings := postingsLists[postingListIndex].Postings
		for postingIndex := range len(postings) {
			termFreq := postings[postingIndex].Tf
			docLength := docRecords[postings[postingIndex].Docid].Doclength
			score := scorer.ATIRE_BM25(termFreq, docLength, idf)
			if score < smallestRSV {
				smallestRSV = score
			}
			if score > largestRSV {
				largestRSV = score
			}
		}
	}

	//update tfs with uniform quantization
	for postingListIndex := range len(postingsLists) {
		idf := scorer.IDF(postingsLists[postingListIndex].Df)
		postings := postingsLists[postingListIndex].Postings
		for postingIndex := range len(postings) {
			termFreq := postings[postingIndex].Tf
			docLength := docRecords[postings[postingIndex].Docid].Doclength
			score := scorer.ATIRE_BM25(termFreq, docLength, idf)
			postings[postingIndex].Tf = quantize(score, bits)
		}
	}
}
