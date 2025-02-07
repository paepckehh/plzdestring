// package main
package main

// import
import (
	"bufio"
	"compress/gzip"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (

	// files
	_file_source    = "plz.csv.gz"
	_file_out_nativ = "../plz.go"

	// components
	_header_nativ    = "// package plzdestring\n// Code generated : German Postcode - DO NOT EDIT MANUALLY: "
	_package_nativ   = "package plzdestring\n\n"
	_map_start_nativ = "// PLZ ... \nvar PLZ = map[string]bool{\n"
	_map_end         = "}\n"
)

func main() {
	// global
	t0 := time.Now()
	feed_chan := make(chan string, 2000)
	co_nativ_chan := make(chan []byte, 2000)

	// collect
	counter, size_nativ := 0, 0
	validation := make(map[string]bool)
	co := sync.WaitGroup{}
	co.Add(1)
	go func() {
		content := make([]byte, 0, 300*1024)
		content = append(content, []byte(_header_nativ+time.Now().Format(time.RFC3339)+"\n")...)
		content = append(content, []byte(_package_nativ)...)
		content = append(content, []byte(_map_start_nativ)...)
		for line := range co_nativ_chan {
			if _, ok := validation[string(line)]; ok {
				continue
			}
			validation[string(line)] = true
			content = append(content, line...)
			counter++
		}
		content = append(content, []byte(_map_end)...)
		size_nativ = len(content) / 1024
		err := os.WriteFile(_file_out_nativ, content, 0o644)
		if err != nil {
			panic("[generator] unable to write generated output file")
		}
		co.Done()
	}()

	// worker
	worker := (runtime.NumCPU() * 2) - 1
	bg := sync.WaitGroup{}
	bg.Add(worker)
	go func() {
		for i := 0; i < worker; i++ {
			go func() {
				for line := range feed_chan {
					s := strings.Split(line, ",")
					if len(s) != 1 {
						outSkip("[wrong number of elements]: ", line)
						continue
					}
					plz, err := strconv.ParseInt(s[0], 10, 64)
					if err != nil {
						outSkip("[unable to parse plz string into number, skipping]: ", line)
						continue
					}
					if plz > 99999 {
						outSkip("[number to large, skipping]: ", line)
					}
					co_nativ_chan <- []byte("\t\"" + s[0] + "\": true,\n")
				}
				bg.Done()
			}()
		}
		bg.Wait()
		close(co_nativ_chan)
	}()

	// feeder
	scanner, total := getFileScanner(_file_source), 0
	for scanner.Scan() {
		feed_chan <- scanner.Text()
		total++
	}
	close(feed_chan)
	co.Wait()
	out("Generated golang nativ code: [" + strconv.Itoa(size_nativ) + "k] [" + _file_out_nativ + "]")
	out("Input lines processed:       " + strconv.Itoa(total))
	out("Output lines processed:      " + strconv.Itoa(counter))
	out("Time needed:                 " + time.Since(t0).String())
}

//
// LITTLE HELPER
//

func out(message string) {
	os.Stdout.Write([]byte(message + "\n"))
}

func outSkip(reason, line string) {
	if len(line) > 80 {
		line = line[:79] + "..."
	}
	out("skipping line " + reason + line)
}

func fl(in float64) string {
	return strconv.FormatFloat(in, 'f', -1, 64)
}

func getFileScanner(filename string) (s *bufio.Scanner) {
	f, err := os.Open(filename)
	if err != nil {
		panic("unable to read db file [" + filename + "] [" + err.Error() + "]")
	}
	r, err := gzip.NewReader(f)
	if err != nil {
		panic("unable to read db file [" + filename + "] [" + err.Error() + "]")
	}
	return bufio.NewScanner(r)
}
