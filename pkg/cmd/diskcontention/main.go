package main

import (
	"context"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
)

const (
	storeDir        = "experiment-data"
	syncDir         = "experiment-sync"
	maxSizeBytes    = 20 * 1024 * 1024 * 1024 // 20 GiB
	parallelWriters = 320
	parallelSyncers = 100
	batchSize       = 1024 * 1024
	keySize         = 16
	valSize         = 128
)

var numWrites uint64
var numBatches uint64

func main() {
	err := os.RemoveAll(storeDir)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	config := base.TempStorageConfigFromEnv(ctx, base.StoreSpec{}, "", maxSizeBytes)
	config.Path = storeDir
	tempStorage, err := engine.NewTempEngine(config)
	if err != nil {
		panic(err)
	}
	for i := 0; i < parallelWriters; i++ {
		go func() {
			err := loadDataToSort(ctx, tempStorage, batchSize)
			panic(err)
		}()
	}

	err = Run(Options{
		Dir:         syncDir,
		Concurrency: parallelSyncers,
	})
	if err != nil {
		panic(err)
	}

	/*
		var numErr int
		var prevNumWrites, prevNumBatches uint64
		start := time.Now()
		lastNow := start
		fmt.Println("_elapsed___errors__ops/sec(inst)___ops/sec(cum)__batch/s(inst)___batch/s(cum)")
		for range time.Tick(time.Second) {
			newNumWrites := atomic.LoadUint64(&numWrites)
			newNumBatches := atomic.LoadUint64(&numBatches)
			now := time.Now()
			elapsed := now.Sub(lastNow)
			fmt.Printf("%8s %8d %14.1f %14.1f %14.1f %14.1f\n",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				numErr,
				float64(newNumWrites-prevNumWrites)/elapsed.Seconds(),
				float64(newNumWrites)/time.Since(start).Seconds(),
				float64(newNumBatches-prevNumBatches)/elapsed.Seconds(),
				float64(newNumBatches)/time.Since(start).Seconds())
			prevNumWrites = newNumWrites
			prevNumBatches = newNumBatches
			lastNow = now
		}
	*/
}

const limit = 16 * 1024 * 1024
const burst = 2 * 1024 * 1024

var rateLimiter = rate.NewLimiter(rate.Limit(limit), burst)

func loadDataToSort(ctx context.Context, tempStorage engine.Engine, batchSize int) error {
	store := engine.NewRocksDBMultiMap(tempStorage)
	defer store.Close(ctx)
	r := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
	key := make([]byte, keySize)
	val := make([]byte, valSize)
	for {
		batch := store.NewBatchWriter()
		for i := 0; i < batchSize; i++ {
			randomBlock(r, key)
			randomBlock(r, val)
			if err := rateLimiter.WaitN(ctx, keySize+valSize); err != nil {
				panic(err)
			}
			if err := batch.Put(key, val); err != nil {
				return err
			}
			atomic.AddUint64(&numWrites, 1)
		}
		if err := batch.Close(ctx); err != nil {
			return err
		}
		atomic.AddUint64(&numBatches, 1)
	}
	return nil
}

func randomBlock(r *rand.Rand, blockData []byte) {
	for i := range blockData {
		blockData[i] = byte(r.Int() & 0xff)
	}
}
