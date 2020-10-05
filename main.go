package main

import (
	"context"
	"fmt"
	goku_client "github.com/corverroos/goku/client/logical"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"
	"imJusySaiyan/imJustSaiyan"
	"sync"
	"time"
)

func main() {
	ctx := context.Background()

	db, err := connectToGokuDB()
	if err != nil {
		log.Error(ctx, errors.Wrap(err, "did not connect"))
	}

	defer db.CloseAll()

	gcl := goku_client.New(db.DB, db.ReplicaDB)
	log.Info(ctx, "Goku client connected")

	key := fmt.Sprintf("key_%d",time.Now().Unix())
	const n = 10

	var ml []*imJustSaiyan.Mutex
	for i := 0; i < n; i++ {
		mu, err := imJustSaiyan.New(gcl, key, fmt.Sprintf("process_%d", i), time.Now().Add(30*time.Second))
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "did not connect"))
		}
		ml = append(ml, mu)
	}

	var wg sync.WaitGroup

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			log.Info(ctx,fmt.Sprintf("%d Locking", i))
			err := ml[i].Lock(ctx)
			if err != nil {
				log.Error(ctx, errors.Wrap(err, fmt.Sprintf("locking err process_%d", i)))
			}

			log.Info(ctx, fmt.Sprintf("%d Locked", i))

			log.Info(ctx,fmt.Sprintf("%d Unlocking", i))
			err = ml[i].Unlock(ctx)
			if err != nil {
				log.Error(ctx, errors.Wrap(err, fmt.Sprintf("unlocking err process_%d", i)))
			}

			log.Info(ctx, fmt.Sprintf("%d Unlocked \n\n", i))

			wg.Done()
		}(i)
	}

	wg.Wait()
}