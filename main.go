package main

import (
    "bufio"
    "crypto/sha1"
    "flag"
    "fmt"
    "hash/adler32"
    "io"
    "math/rand"
    "os"
)

const (
    READ_BUFFER_SIZE  = 1 * 1024 * 1024
    WRITE_BUFFER_SIZE = 1 * 1024 * 1024
)

var (
    blockSize int
    inFile    string
    outFile   string
    randTail  bool
)

var (
    blockList   = make([]*BlockData, 0)
    fileMap     = make(map[string][]byte)
    checksumMap = make(map[string]string)
)

type BlockData struct {
    Key   string
    Index int64
}

func main() {
    flag.IntVar(&blockSize, "blockSize", 2048, "")
    flag.StringVar(&inFile, "inFile", "", "")
    flag.StringVar(&outFile, "outFile", "", "")
    flag.BoolVar(&randTail, "randTail", false, "")
    flag.Parse()

    f, err := os.Open(inFile)
    if err != nil {
        panic(err)
    }
    defer f.Close()

    o, err := os.Create(outFile)
    if err != nil {
        panic(err)
    }
    defer o.Close()

    cs := adler32.New()
    sha := sha1.New()
    reader := bufio.NewReaderSize(f, READ_BUFFER_SIZE)
    writer := bufio.NewWriterSize(o, WRITE_BUFFER_SIZE)
    defer writer.Flush()

    for i := int64(0); ; i++ {
        buffer := make([]byte, blockSize)
        count, err := reader.Read(buffer)
        if err != nil && err != io.EOF {
            panic(err)
        }

        if err == io.EOF {
            break
        }

        sha.Reset()
        sha.Write(buffer[:count])
        key := fmt.Sprintf("%X", sha.Sum(nil))

        cs.Reset()
        cs.Write(buffer[:count])
        checksumMap[key] = fmt.Sprintf("%X", cs.Sum(nil))

        _, ok := fileMap[key]
        if !ok {
            fileMap[key] = buffer[:count]
        }

        blockList = append(blockList, &BlockData{Key: key, Index: i})
    }

    shuffle(blockList)

    // generate random file layout
    cursor := int64(0)
    for i := 0; i < len(blockList); i++ {
        // insert padding
        padBuffer := make([]byte, rand.Intn(128))
        for x := range padBuffer {
            padBuffer[x] = byte(rand.Intn(100))
        }
        writer.Write(padBuffer)

        fmt.Println(padBuffer)

        fmt.Printf("Add %d new bytes\n", len(padBuffer))
        cursor += int64(len(padBuffer))

        // insert a random block
        key := blockList[i].Key
        writer.Write(fileMap[key])

        fmt.Printf(
            "[%d] Insert block %s.%s from offset %d. %d bytes\n",
            cursor,
            checksumMap[key],
            key,
            blockList[i].Index*2048,
            len(fileMap[key]),
        )
        cursor += int64(len(fileMap[key]))
    }

    // insert one last random padding at the end
    if randTail {
        padBuffer := make([]byte, rand.Intn(128))
        for x := range padBuffer {
            padBuffer[x] = byte(rand.Intn(100))
        }
        writer.Write(padBuffer)

        fmt.Println(padBuffer)
    }

    // make sure everything is flushed to file
    writer.Flush()
    fmt.Printf("%d blocks processed\n", len(blockList))
}

func shuffle(a []*BlockData) {
    for i := range a {
        j := rand.Intn(i + 1)
        a[i], a[j] = a[j], a[i]
    }
}
