package main

import (
 "github.com/couchbase/go-couchbase"
 "gopkg.in/redis.v4"
 _ "github.com/lib/pq"
 "crypto/md5"
 "bufio"
 "database/sql"
 "encoding/json"
 "strings"
 "os"
 "flag"
 "log"
 "math/rand"
 "fmt"
 "time"
 "strconv"
 "sync"
 "sync/atomic"
)

type User struct {
   Id int `json:"id"`
   Age int `json:"age"`
   LastName string `json:"last_name"`
   FirstName string `json:"first_name"`
   Email string `json:"email"`
   Password string `json:"password"`
   CityId int `json:"city_id"`
   Gender string
}

type CouchClient struct {
  client *couchbase.Bucket
}

type RedisClient struct {
  client *redis.Client
}

type PGClient struct {
  client *sql.DB
}

type Client interface {
  Set(key string, value string) (error)
  Get(key string) (string, error)
}


func (c *CouchClient) Set(key string, value string) (error) {
   err := c.client.Set(key, 0, getUser())
   if err != nil {
     log.Println(err)
   }
   return err
}

func (c *CouchClient) Get(key string) (string, error) {
   var err error
   var bytes []byte
   user := new(User)
   err = c.client.Get(key, user)

   if err == nil {
     bytes, err = json.Marshal(user)
   }

   return string(bytes), err
}

func (c *RedisClient) Set(key string, value string) (error) {
   err := c.client.Set(key, value, 0).Err()
   return err
}

func (c *RedisClient) Get(key string) (string, error) {
   v, err := c.client.Get(key).Result()
   return v, err
}

func (c *PGClient) Set(key string, value string) (error) {
   var user User
   var err error = json.Unmarshal([]byte(value), &user)
   rows, err := c.client.Query("INSERT INTO users(first_name, last_name, email, city_id, gender, password) VALUES ($1, $2, $3, $4, $5, $6)", user.FirstName, user.LastName, user.Email, user.CityId, user.Gender, user.Password)
   if err == nil {
     for rows.Next() {
     }
   }
   return err
}

func (c *PGClient) Get(key string) (string, error) {
   var id int
   var err error
   var str string
   var rows *sql.Rows
   var user User

   id, err = strconv.Atoi(key[strings.Index(key, ":")+1:])

   if err == nil {
     rows, err = c.client.Query("SELECT id, first_name , last_name, email, gender, city_id, password FROM users WHERE id = $1", id)
   }

   if err == nil {
     for rows.Next() {
        rows.Scan(&user.Id, &user.FirstName, &user.LastName, &user.Email, &user.Gender, &user.CityId, &user.Password)
     }

     bytes, err := json.Marshal(user)
     if err == nil {
       str = string(bytes)
     }
   }

   return str, err
}


func worker(linkChan chan int64, wg *sync.WaitGroup, processor func(int64)) {
  // Decreasing internal counter for wait-group as soon as goroutine finishes
  defer wg.Done()

  for id := range linkChan {
    processor(id)
  }

}

var words []string
func readLines(path string) ([]string, error) {
  file, err := os.Open(path)
  if err != nil {
    return nil, err
  }
  defer file.Close()

  var lines []string
  scanner := bufio.NewScanner(file)
  for scanner.Scan() {
    lines = append(lines, scanner.Text())
  }
  return lines, scanner.Err()
}

func getUser() User {
   firstName := words[rand.Intn(len(words)-1)]
   lastName := words[rand.Intn(len(words)-1)]
   password := fmt.Sprintf("%x", md5.Sum([]byte(firstName + lastName + fmt.Sprintf("%d", rand.Intn(1e5)))))
   return User{0, rand.Intn(90)+1, firstName, lastName, firstName + "." + lastName + "@gmail.com", password, rand.Intn(1e5)+1, "M"}
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
  var client Client

  requests := flag.Int("r", 20000, "number of requests to make")
  threads := flag.Int("c", 20, "number of threads")
  doGet := flag.Bool("get", false, "Reads test")
  test := flag.String("test", "redis", "number of requests to make")
  flag.Parse()

	words, err = readLines("./words.shuffled.txt")
	if err != nil {
 		log.Fatal("Problem reading words file.")
	}

  // TODO: configure with properties
  switch(*test) {
    case "redis":
      client = &RedisClient{redis.NewClient(&redis.Options{
            Addr:     "localhost:6379",
            Password: "", // no password set
            DB:       0,  // use default DB
      })}
    case "couchbase":
        const BUCKET = "__test_bucket__"
        c, err := couchbase.Connect("http://localhost:8091/")
        if err != nil {
            log.Fatalf("Error connecting:  %v", err)
        }

        pool, err := c.GetPool("default")
        if err != nil {
            log.Fatalf("Error getting pool:  %v", err)
        }

        bucket, err := pool.GetBucket(BUCKET)
        if err != nil {
            log.Fatalf("Error getting bucket:  %v", err)
        }

        client = &CouchClient{bucket}
    case "psql":
      fallthrough
    case "postgres":
      str := os.Getenv("DATABASE_URL") + "?sslmode=disable"
      db, err := sql.Open("postgres", str)
      if err != nil {
        log.Fatal(err)
        os.Exit(1)
      }
      client = &PGClient{db}
    default:
      log.Fatalf(`Test "%s", not recognized`, *test)
      os.Exit(1)
  }

  log.Println(fmt.Sprintf("Testing %s using %d requests using %d threads.", *test, *requests, *threads))

  var failureCount uint64 = 0

  log.Println("Doing insert test...")
  DoBenchmark("insert", *requests, *threads, func(id int64) {
       bytes, err := json.Marshal(getUser())

       if err == nil {
         err = client.Set("users:" + strconv.FormatInt(id, 10), string(bytes))
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
        _, err := client.Get("users:" + strconv.FormatInt(id, 10))
        if err != nil {
            atomic.AddUint64(&failureCount, 1)
        }
    })

    log.Printf("%d failures.\n", atomic.LoadUint64(&failureCount))
  }
}
