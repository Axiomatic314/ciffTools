package quantize

import (
	"math"
)

type rankingFunction struct {
	k1, b            float64
	numDocs          int32
	averageDocLength float64
}

func (ranker rankingFunction) IDF(docFreq int64) float64 {
	return math.Log((float64(ranker.numDocs)) / float64(docFreq))
}

func (ranker rankingFunction) ATIRE_BM25(termFreq int32, docLength int32, idf float64) float64 {
	top := (ranker.k1 + 1) * float64(termFreq)
	termWeight := top / (ranker.k1*(1-ranker.b+ranker.b*(float64(docLength)/ranker.averageDocLength)) + float64(termFreq))
	return idf * termWeight
}
