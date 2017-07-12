package main

import (
 "encoding/json"
 "crypto/md5"
 "math/rand"
 "flag"
 "log"
 "fmt"
 "time"
 "sync"
 "strconv"
 "sync/atomic"
 "github.com/base698/benchmarks/benchmarks/file"
 "github.com/base698/benchmarks/benchmarks/client"
)


func GetUser(words *[]string) client.User {
	 wordsArr := *words
   firstName := wordsArr[rand.Intn(len(wordsArr)-1)]
   lastName := wordsArr[rand.Intn(len(wordsArr)-1)]
   password := fmt.Sprintf("%x", md5.Sum([]byte(firstName + lastName + fmt.Sprintf("%d", rand.Intn(1e5)))))
   return client.User{0, rand.Intn(90)+1, firstName, lastName, firstName + "." + lastName + "@gmail.com", password, rand.Intn(1e5)+1, "M"}
}

func worker(linkChan chan int64, wg *sync.WaitGroup, processor func(int64)) {
  // Decreasing internal counter for wait-group as soon as goroutine finishes
  defer wg.Done()

  for id := range linkChan {
    processor(id)
  }

}

func DoBenchmark(name string, numDocs int, threads int, operation (func(id int64))) {
  opCh := make(chan int64)
  failureCh := make(chan int64)
  wg := new(sync.WaitGroup)

  start := time.Now().UnixNano() / int64(time.Millisecond)

  // Adding routines to workgroup and running then
  for i := 0; i < threads; i++ {
    wg.Add(1)
    go worker(opCh, wg, operation) // func(id int64) {})
  }

  // var wg sync.WaitGroup
  // Processing all links by spreading them to `free` goroutines
  count := int64(0)

  for i := 0; i < numDocs; i++ {
    count += 1
    opCh <- count
  }

  // Closing channel (waiting in goroutines won't continue any more)
  close(opCh)
  close(failureCh)

  // Waiting for all goroutines to finish (otherwise they die as main routine dies)
  wg.Wait()

  end := time.Now().UnixNano() / int64(time.Millisecond)
  log.Println( fmt.Sprintf("%1.2f %s OPS (ops/second)", float64(1000) * (float64(count) / (float64(end) - float64(start))),name))
}



func main() {
  var err error
  var dbClient client.Client
	var words []string

  requests := flag.Int("r", 20000, "number of requests to make")
  threads := flag.Int("c", 20, "number of threads")
  doGet := flag.Bool("get", false, "Reads test")
  test := flag.String("test", "redis", "number of requests to make")
  flag.Parse()

	dbClient = client.GetClient(*test)

	words, err = file.ReadLines("./words.shuffled.txt")

	if err != nil {
 		log.Fatal("Problem reading words file.")
	}

  log.Println(fmt.Sprintf("Testing %s using %d requests using %d threads.", *test, *requests, *threads))

  var failureCount uint64 = 0

  log.Println("Doing insert test...")
  DoBenchmark("insert", *requests, *threads, func(id int64) {
       bytes, err := json.Marshal(GetUser(&words))

       if err == nil {
         err = dbClient.Set("users:" + strconv.FormatInt(id, 10), string(bytes))
       }

       if err != nil {
         atomic.AddUint64(&failureCount, 1)
       }
  })

  log.Printf("%d failures.\n", atomic.LoadUint64(&failureCount))
  failureCount = 0

  if *doGet {
    log.Println("Doing read test...")
    DoBenchmark("get", *requests, *threads, func(id int64) {
        _, err := dbClient.Get("users:" + strconv.FormatInt(id, 10))
        if err != nil {
            atomic.AddUint64(&failureCount, 1)
        }
    })

    log.Printf("%d failures.\n", atomic.LoadUint64(&failureCount))
  }
}
