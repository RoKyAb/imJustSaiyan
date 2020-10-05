package imJustSaiyan

import (
	"context"
	"fmt"
	goku_test "github.com/corverroos/goku/test"
	"github.com/luno/jettison/jtest"
	"github.com/luno/jettison/log"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestLockUnlock(t *testing.T) {
	gc, _ := goku_test.SetupForTesting(t)

	mu, err := New(gc, "key1", "process1", time.Now().Add(2*time.Second))
	jtest.RequireNil(t, err)

	ctx := context.Background()
	err = mu.Lock(ctx)
	jtest.RequireNil(t, err)

	err = mu.Unlock(ctx)
	jtest.RequireNil(t, err)
}

func Test_SingleKey_LockWait(t *testing.T) {
	gc, _ := goku_test.SetupForTesting(t)

	key := fmt.Sprintf("key_%d",rand.Int())
	const n = 30

	var ml []*Mutex
	for i := 0; i < n; i++ {
		mu, err := New(gc, key, fmt.Sprintf("process_%d", i), time.Now().Add(30*time.Second))
		assert.NoError(t, err)
		ml = append(ml, mu)
	}

	var wg sync.WaitGroup

	ctx := context.Background()
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			log.Info(ctx,fmt.Sprintf("%d Locking", i))
			err := ml[i].Lock(ctx)
			assert.NoError(t, err)
			log.Info(ctx, fmt.Sprintf("%d Locked", i))

			time.Sleep(50 * time.Millisecond)

			log.Info(ctx,fmt.Sprintf("%d Unlocking", i))
			err = ml[i].Unlock(ctx)
			assert.NoError(t, err)
			log.Info(ctx, fmt.Sprintf("%d Unlocked \n\n", i))

			wg.Done()
		}(i)
	}

	wg.Wait()
}

func Test_SingleKey_LockWait_MultipleDBConns(t *testing.T) {
	gc, dbc := goku_test.SetupForTesting(t)
	dbc.SetMaxOpenConns(100)

	key := fmt.Sprintf("key_%d",rand.Int())
	const n = 10

	var ml []*Mutex
	for i := 0; i < n; i++ {
		mu, err := New(gc, key, fmt.Sprintf("process_%d", i), time.Now().Add(5*time.Second))
		assert.NoError(t, err)
		ml = append(ml, mu)
	}

	var wg sync.WaitGroup

	ctx := context.Background()
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			log.Info(ctx,fmt.Sprintf("%d Locking", i))
			err := ml[i].Lock(ctx)
			assert.NoError(t, err)
			log.Info(ctx, fmt.Sprintf("%d Locked", i))

			time.Sleep(200 * time.Millisecond)

			log.Info(ctx,fmt.Sprintf("%d Unlocking", i))
			err = ml[i].Unlock(ctx)
			assert.NoError(t, err)
			log.Info(ctx, fmt.Sprintf("%d Unlocked \n\n", i))

			wg.Done()
		}(i)
	}

	wg.Wait()
}