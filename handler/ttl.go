package handler

import (
	"context"
	"time"

	"github.com/prometheus/pushgateway/storage"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
)

type token struct{}

type ttlEntry struct {
	labels map[string]string
	expiry time.Time
}

// TTL will manage metric group entries and purge them according to their
// provided TTL.
type TTL struct {
	ms    storage.MetricStore
	beats map[uint64]ttlEntry
	sem   chan token
}

// NewTTL will return a new TTL.
func NewTTL(ms storage.MetricStore) *TTL {
	return &TTL{
		ms:    ms,
		beats: make(map[uint64]ttlEntry),
		sem:   make(chan token, 1),
	}
}

// Beat will mark the given label set as visited, and will record the given ttl
// and expiry to ensure that the entry is deleted at least at that time.
func (t *TTL) Beat(ctx context.Context, labels map[string]string, ttl time.Duration) error {
	select {
	case t.sem <- token{}:
		key := model.LabelsToSignature(labels)
		t.beats[key] = ttlEntry{
			labels: labels,
			expiry: time.Now().Add(ttl),
		}
		<-t.sem
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PurgeExpired will delete the entries that have expired from the metrics
// group.
func (t *TTL) PurgeExpired(ctx context.Context) error {
	select {
	case t.sem <- token{}:
		now := time.Now()
		for key, beat := range t.beats {
			if now.After(beat.expiry) {
				log.Debug("deleting expired entry")

				// Write out now to delete this entry from the metrics store.
				t.ms.SubmitWriteRequest(storage.WriteRequest{
					Labels:    beat.labels,
					Timestamp: now,
				})

				// Remove the entry from the map.
				delete(t.beats, key)
			}
		}
		<-t.sem
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
