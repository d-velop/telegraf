package resque

import (
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal/tls"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type Resque struct {
	Servers []string
	tls.ClientConfig

	clients     []Client
	initialized bool
}

type Client interface {
	Info() *redis.StringCmd
	Get(string) *redis.StringCmd
	HGet(key, field string) *redis.StringCmd
	SMembers(string) *redis.StringSliceCmd
	LLen(string) *redis.IntCmd
	Time() *redis.TimeCmd
	BaseTags() map[string]string
}

type RedisClient struct {
	client *redis.Client
	tags   map[string]string
}

func (r *RedisClient) Info() *redis.StringCmd {
	return r.client.Info()
}

func (r *RedisClient) Get(key string) *redis.StringCmd {
	return r.client.Get(key)
}

func (r *RedisClient) HGet(key, field string) *redis.StringCmd {
	return r.client.HGet(key, field)
}

func (r *RedisClient) SMembers(key string) *redis.StringSliceCmd {
	return r.client.SMembers(key)
}

func (r *RedisClient) LLen(key string) *redis.IntCmd {
	return r.client.LLen(key)
}

func (r *RedisClient) Time() *redis.TimeCmd {
	return r.client.Time()
}

func (r *RedisClient) BaseTags() map[string]string {
	tags := make(map[string]string)
	for k, v := range r.tags {
		tags[k] = v
	}
	return tags
}

var sampleConfig = `
  ## specify servers via a url matching:
  ##  [protocol://][:password]@address[:port]
  ##  e.g.
  ##    tcp://localhost:6379
  ##    tcp://:password@192.168.99.100
  ##    unix:///var/run/redis.sock
  ##
  ## If no servers are specified, then localhost is used as the host.
  ## If no port is specified, 6379 is used
  servers = ["tcp://localhost:6379"]

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = true
`

func (r *Resque) SampleConfig() string {
	return sampleConfig
}

func (r *Resque) Description() string {
	return "Read resque metrics from one or many redis servers"
}

func (r *Resque) init(acc telegraf.Accumulator) error {
	if r.initialized {
		return nil
	}

	if len(r.Servers) == 0 {
		r.Servers = []string{"tcp://localhost:6379"}
	}

	r.clients = make([]Client, len(r.Servers))

	for i, serv := range r.Servers {
		if !strings.HasPrefix(serv, "tcp://") && !strings.HasPrefix(serv, "unix://") {
			log.Printf("W! [inputs.redis]: server URL found without scheme; please update your configuration file")
			serv = "tcp://" + serv
		}

		u, err := url.Parse(serv)
		if err != nil {
			return fmt.Errorf("Unable to parse to address %q: %v", serv, err)
		}

		password := ""
		if u.User != nil {
			pw, ok := u.User.Password()
			if ok {
				password = pw
			}
		}

		var address string
		if u.Scheme == "unix" {
			address = u.Path
		} else {
			address = u.Host
		}

		tlsConfig, err := r.ClientConfig.TLSConfig()
		if err != nil {
			return err
		}

		client := redis.NewClient(
			&redis.Options{
				Addr:      address,
				Password:  password,
				Network:   u.Scheme,
				PoolSize:  1,
				TLSConfig: tlsConfig,
			},
		)

		tags := map[string]string{}
		if u.Scheme == "unix" {
			tags["socket"] = u.Path
		} else {
			tags["server"] = u.Hostname()
			tags["port"] = u.Port()
		}

		r.clients[i] = &RedisClient{
			client: client,
			tags:   tags,
		}
	}

	r.initialized = true
	return nil
}

// Reads stats from all configured servers accumulates stats.
// Returns one of the errors encountered while gather stats (if any).
func (r *Resque) Gather(acc telegraf.Accumulator) error {
	if !r.initialized {
		err := r.init(acc)
		if err != nil {
			return err
		}
	}

	var wg sync.WaitGroup

	for _, client := range r.clients {
		wg.Add(1)
		go func(client Client) {
			defer wg.Done()
			acc.AddError(r.gatherServer(client, acc))
		}(client)
	}

	wg.Wait()
	return nil
}

func (r *Resque) gatherServer(client Client, acc telegraf.Accumulator) error {
	err := gatherJobsAndWorkersCount(client, acc)
	if err != nil {
		return err
	}
	err = gatherQueueSizes(client, acc)
	if err != nil {
		return err
	}
	return nil
}

func gatherJobsAndWorkersCount(client Client, acc telegraf.Accumulator) error {
	fields := make(map[string]interface{})

	val, err := countFailedJobs(client)
	if err != nil {
		return err
	}
	fields["failed_jobs"] = val

	val, err = countProcessedJobs(client)
	if err != nil {
		return err
	}
	fields["processed_jobs"] = val

	val, err = countWorkers(client)
	if err != nil {
		return err
	}
	fields["workers"] = val

	acc.AddFields("resque", fields, client.BaseTags())
	return nil
}

func countWorkers(client Client) (int, error) {
	redisTime, err := client.Time().Result()
	if err != nil {
		return 0, err
	}
	timeToBeat := redisTime.Add(-90 * time.Second)
	fmt.Println(timeToBeat)

	workerIDs, err := client.SMembers("resque::workers").Result()
	if err != nil {
		return 0, err
	}
	workerCount := 0
	for _, workerID := range workerIDs {
		timestamp, err := client.HGet("resque::workers:heartbeat", workerID).Result()
		if err == redis.Nil {
			break
		}
		if err != nil {
			return workerCount, err
		}
		heartbeat, err := time.Parse(time.RFC3339, timestamp)
		if err != nil {
			return workerCount, err
		}
		if heartbeat.After(timeToBeat) {
			workerCount++
		}
	}
	return workerCount, nil
}

func countFailedJobs(client Client) (int, error) {
	val, err := client.Get("resque::stat:failed").Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, err
	}
	return strconv.Atoi(val)
}

func countProcessedJobs(client Client) (int, error) {
	val, err := client.Get("resque::stat:processed").Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, err
	}
	return strconv.Atoi(val)
}

func gatherQueueSizes(client Client, acc telegraf.Accumulator) error {
	val, err := countQueueSizes(client)
	if err != nil {
		return err
	}
	for k, v := range val {
		fields := make(map[string]interface{})
		fields["queue_size"] = v
		tags := client.BaseTags()
		tags["queue"] = k
		acc.AddFields("resque", fields, tags)
	}
	return nil
}

func countQueueSizes(client Client) (map[string]int64, error) {
	queueSizes := make(map[string]int64)

	val, err := client.SMembers("resque::queues").Result()
	if err != nil {
		return queueSizes, err
	}
	for _, queue := range val {
		val, err := client.LLen("resque::queue:" + queue).Result()
		if err != nil {
			return queueSizes, err
		}
		queueSizes[queue] = val
	}
	return queueSizes, nil
}

func init() {
	inputs.Add("resque", func() telegraf.Input {
		return &Resque{}
	})
}
