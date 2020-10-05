package imJustSaiyan

import (
	"context"
	"fmt"
	"github.com/corverroos/goku"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"math/rand"
	"time"
)

type Mutex struct {
	cl 			goku.Client
	ExpiresAt   time.Time
	lockName 	string
	processID	string
	cursor		string
	lastEvent	*reflex.Event
	prevVersion	int64
}

func New(cl goku.Client, lockName, processID string, expires time.Time) (*Mutex, error) {
	if lockName == "" {
		return nil, errors.New("no lock name provided")
	}

	m := Mutex{
		cl:        cl,
		ExpiresAt: expires,
		processID: processID,
		lockName:  lockName,
		prevVersion: 2,
	}

	return &m, nil
}

// Lock attempts to acquire the mutex. If the mutex is already held, Lock
// will block until the mutex becomes available.
func (m *Mutex) Lock(ctx context.Context) error {
	for {
		sc, err := m.cl.Stream(m.lockName)(ctx, m.cursor, reflex.WithStreamToHead())
		if err != nil {
			return err
		}

		event, err := sc.Recv()
		if errors.Is(err, reflex.ErrHeadReached) {
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		} else if err != nil {
			return err
		}

		if event != nil {
			m.lastEvent = event
			m.cursor = event.ID

		}

		kv, err := m.cl.Get(ctx, m.lockName)
		if errors.Is(err, goku.ErrNotFound) {
			// the mutex may be available
			if m.lastEvent == nil {
				options := []goku.SetOption{
					goku.WithExpiresAt(m.ExpiresAt),
					goku.WithCreateOnly(),
				}

				ok, err := m.TryLock(ctx, options)
				if ok || err != nil {
					return err
				}

			} else if reflex.IsType(m.lastEvent.Type , goku.EventTypeExpire) || reflex.IsType(m.lastEvent.Type, goku.EventTypeDelete) {
				options := []goku.SetOption{
					goku.WithExpiresAt(m.ExpiresAt),
					goku.WithPrevVersion(m.prevVersion +1 ),
				}

				ok, err := m.TryLock(ctx, options)
				if ok || err != nil {
					return err
				}
			}
		} else if err != nil {
			return err
		}

		if kv.Version != 0 {
			m.prevVersion = kv.Version
		}
	}
}

// TryLock attempts to acquire the mutex and returns immediately
// with true if it was successful.
// If the mutex is already held, TryLock will return false and no error.
func (m *Mutex) TryLock(ctx context.Context, options []goku.SetOption) (bool, error) {
	err := m.cl.Set(ctx, m.lockName, []byte(m.processID), options...)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, goku.ErrConditional) {
		return false, nil
	}

	if errors.Is(err, goku.ErrUpdateRace) {
		fmt.Println(m.prevVersion, m.processID, m.cursor, m.lastEvent)
		return false, nil
	}

	return false, errors.Wrap(err, "try locking")
}

// Unlock releases the mutex by deleting the corresponding key.
func (m *Mutex) Unlock(ctx context.Context) error {
	return m.cl.Delete(ctx, m.lockName)
}