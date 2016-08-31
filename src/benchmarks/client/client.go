package client

import (
 "log"
 "os"
 "strconv"
 "github.com/couchbase/go-couchbase"
 "strings"
 "gopkg.in/redis.v4"
 _ "github.com/lib/pq"
 "database/sql"
 "encoding/json"
 "github.com/gocql/gocql"
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

type CassandraClient struct {
  client *gocql.Session
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

func (c *CassandraClient) Set(key string, value string) (error) {
   var user User
   var err error = json.Unmarshal([]byte(value), &user)

	if err != nil {
		 return err
	}
  cql := "INSERT INTO users(id, first_name, last_name, email, city_id, gender, password) VALUES (?,?,?,?,?,?,?)"   
  if err = c.client.Query(cql,
	 gocql.TimeUUID(), user.FirstName, user.LastName, user.Email, user.CityId, user.Gender, user.Password).Exec(); err != nil {
       return err
  }

	return nil
}

func (c *CassandraClient) Get(key string) (string, error) {
	  var text string
    cql := "SELECT id, first_name , last_name, email, gender, city_id, password FROM users WHERE id = $1"
	  err := c.client.Query(cql,
        key).Consistency(gocql.One).Scan(&text)
	  return text, err
}

func (c *CouchClient) Set(key string, value string) (error) {
   err := c.client.Set(key, 0, value)
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


func GetClient(test string) Client {
	var client Client
  // TODO: configure with properties
  switch(test) {
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
	case "cassandra":
		fallthrough
	case "cassy":
		    // connect to the cluster
    cluster := gocql.NewCluster("localhost")
		cluster.ProtoVersion = 3
    cluster.Keyspace = "example"
    cluster.Consistency = gocql.Quorum
    sess, err := cluster.CreateSession()
		if err != nil {
			log.Fatal("Cassandra couldn't connect")
		}
		client = &CassandraClient{sess}
    
  default:
      log.Fatalf(`Test "%s", not recognized`, test)
      os.Exit(1)
  }

	return client

}
