package pool

import (
	"context"
	"fmt"
	"github.com/bitleak/go-redis-pool/v2/hashkit"
	"github.com/go-redis/redis/v8"
	"math/rand"
	"testing"
	"time"
)

func Benchmark_doMultiKeys(b *testing.B) {
	fn := makeMultiKeyFn()
	keys := []string{"hello", "world", "how", "are", "you", "doing", "today?", "😊"}

	for _, shardsN := range []int{4, 8, 16, 32} {
		factory, err := setupFactory(shardsN)
		if err != nil {
			b.Fatalf("setup factory for %d shards failed with error: %s", shardsN, err)
		}

		b.Run(fmt.Sprintf("old: shardsN = %d", shardsN), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = factory.doMultiKeysOld(fn, keys...)
			}
		})

		b.Run(fmt.Sprintf("new: shardsN = %d", shardsN), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = factory.doMultiKeys(fn, keys...)
			}
		})
	}
}

func setupFactory(shardsN int) (*ShardConnFactory, error) {
	shards := make([]*HAConfig, 0, 0)
	for i := 0; i < shardsN; i++ {
		shards = append(shards, &HAConfig{
			Master: fmt.Sprintf("127.0.0.%d:6379", i),
		})
	}

	return NewShardConnFactory(&ShardConfig{
		Shards:         shards,
		DistributeType: DistributeByModular,
		HashFn:         hashkit.Xxh3,
	})
}

func makeMultiKeyFn() multiKeyFn {
	return func(factory *ShardConnFactory, keys ...string) redis.Cmder {
		// network delay and operation execution simulation
		time.Sleep(time.Millisecond + time.Duration(500*rand.Float64())*time.Microsecond)
		return redis.NewStringCmd(context.Background(), keys)
	}
}
