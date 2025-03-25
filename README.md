# ciffTools
Tools for working with the Common Index File Format [(CIFF)](https://github.com/osirrc/ciff). 

Currently allows the user to:
- quantize a CIFF index.
- write out human-readable dumps of the dictionary, postings, docRecords, and/or header from any CIFF.

## Building the Project
After cloning the repo, simply build the executable with:
```
go build
```

## Usage
You must specify a CIFF file with `-ciffFilePath` for the program to run. 

The output directory may be changed with the `-outputDirectory` flag, the default is `output`.

### Quantizing
Quantize the given CIFF by using the `-writeCiff` flag. Optionally, you may specify the `k1` and `b` parameters for BM25. The defaults are: `k1 = 0.9` and `b = 0.4`.
```
./ciffTools -ciffFilePath <path-to-ciff> -k1 0.82 -b 0.68 -writeCiff
```

### Write out Human-Readable CIFF
Note this is done after any quantization. If `-writeCiff` is not specified, it will use the original CIFF.
- `-writeHeader` --- write header to `output.header`
- `-writeDict` --- write dictionary to `output.dict`
- `-writeDocRecords` --- write docRecords to `output.docRecords`
- `-writePostings` --- write postings to `output.postings`

Example:
```
./ciffTools -ciffFilePath <path-to-ciff> -writeCiff -writePostings -writeDict
```


## Disclaimer 
This tool uses an absurd amount of RAM to quantize CIFFs. To quantize CIFFs for Robust04, Gov2, and MSMARCO I used a machine with 400GB of RAM. Although I have been tempted to rewrite this tool for a machine with lower RAM, I have not had the time --- and it would be undoubtedly slower.

