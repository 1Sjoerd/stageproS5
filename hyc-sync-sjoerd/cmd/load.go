/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"bitbucket.org/tsg-eos/hyrule/config"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

// loadCmd represents the load command
var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		emitter, err := goka.NewEmitter(config.Brokers, IoTStream, new(codec.Bytes))
		if err != nil {
			log.Fatalf("error creating emitter: %v", err)
		}
		defer emitter.Finish()

		rdb := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})

		// walk over ./data directory and load all files
		var walkDir func(dir string) error

		walkDir = func(dir string) error {
			entries, err := os.ReadDir(dir)
			if err != nil {
				return err
			}

			for _, entry := range entries {
				if entry.IsDir() {
					walkDir(path.Join(dir, entry.Name()))
				}

				// try read file line by line
				file, err := os.Open(path.Join(dir, entry.Name()))
				if err != nil {
					log.Fatalf("error opening file: %v", err)
				}
				defer file.Close()

				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					// try parse as json
					var data map[string]interface{}
					if err := json.Unmarshal(scanner.Bytes(), &data); err != nil {
						fmt.Println("error parsing json: ", err)
						continue
					}

					if deduplicationId, ok := data["deduplicationId"]; ok {
						key := deduplicationId.(string)

						result, err := rdb.Get(context.Background(), key).Result()
						if err != nil && err != redis.Nil {
							log.Fatalf("error getting redis key: %v", err)
						}

						if result != "" {
							fmt.Printf("duplicate message detected for device %s\n", key)
							continue
						}

						_, err = rdb.Set(context.Background(), key, "1", 1*time.Minute).Result()
						if err != nil {
							log.Fatalf("error setting redis key: %v", err)
						}
					}

					// remove deduplicationId
					delete(data, "deduplicationId")

					// time, err := time.Parse(time.RFC3339Nano, data["time"].(string))
					// if err != nil {
					// 	log.Fatalf("error parsing time: %v", err)
					// }

					devEui := data["deviceInfo"].(map[string]interface{})["devEui"].(string)

					// fmt.Printf("devEui: %s\ttime: %v\n", devEui, time)

					bytes, err := json.Marshal(data)
					if err != nil {
						log.Fatalf("error marshalling data: %v", err)
					}

					emitter.Emit(devEui, bytes)
				}
			}

			return nil
		}

		err = walkDir("./data")
		if err != nil {
			log.Fatalf("error walking directory: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(loadCmd)
}
