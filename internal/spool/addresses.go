package spool

import (
	"bytes"
	"errors"
	"fmt"
	"net/netip"
	"slices"

	bolt "go.etcd.io/bbolt"
)

// The agent's target addresses (review, round six): the address each name
// target last resolved to, so that after a restart a target whose name
// cannot be resolved (DNS down with the network) is still probed and its
// outage recorded. They are in a bucket of their own, which the first
// SaveAddress creates, keyed by the agent's keys (the lookup's network and
// the name), each value the address in netip.Addr's binary form. Not in
// meta, whose values are the spool's counters. Each operation is one
// transaction, atomic like every other, and none touches the records, seq
// or cursors; at the size bound or on a full disk it makes room as any
// operation does (see change).
var bucketAddresses = []byte("addresses")

// Addresses returns the addresses saved with SaveAddress, by key. A value
// that does not decode is left out.
func (s *Spool) Addresses() (map[string]netip.Addr, error) {
	db, err := s.acquire()
	if err != nil {
		return nil, fmt.Errorf("spool addresses: %w", err)
	}
	out := map[string]netip.Addr{}
	err = guard(func() error {
		return db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketAddresses)
			if b == nil {
				return nil
			}
			return b.ForEach(func(k, v []byte) error {
				var a netip.Addr
				if v != nil && a.UnmarshalBinary(v) == nil && a.IsValid() {
					out[string(k)] = a
				}
				return nil
			})
		})
	})
	s.release()
	if err != nil {
		if errors.Is(err, errCorrupt) {
			s.replace(db, err)
		}
		return nil, fmt.Errorf("spool addresses: %w", err)
	}
	return out, nil
}

// SaveAddress saves addr under key. If key has addr already, nothing is
// written: the agent saves every lookup, and the address rarely changes.
func (s *Spool) SaveAddress(key string, addr netip.Addr) error {
	if !addr.IsValid() {
		return errors.New("spool save address: invalid address")
	}
	val, err := addr.MarshalBinary()
	if err != nil {
		return fmt.Errorf("spool save address: %w", err)
	}
	err = s.change(s.update, func(tx *bolt.Tx, _ *txState) error {
		if b := tx.Bucket(bucketAddresses); b != nil && bytes.Equal(b.Get([]byte(key)), val) {
			return errNothing
		}
		b, err := tx.CreateBucketIfNotExists(bucketAddresses)
		if err != nil {
			return err
		}
		return b.Put([]byte(key), val)
	})
	if err != nil && !errors.Is(err, errNothing) {
		return fmt.Errorf("spool save address: %w", err)
	}
	return nil
}

// KeepAddresses deletes the saved addresses of every key not in keys.
func (s *Spool) KeepAddresses(keys []string) error {
	err := s.change(s.update, func(tx *bolt.Tx, _ *txState) error {
		b := tx.Bucket(bucketAddresses)
		if b == nil {
			return errNothing
		}
		var stale [][]byte
		if err := b.ForEach(func(k, _ []byte) error {
			if !slices.Contains(keys, string(k)) {
				stale = append(stale, slices.Clone(k))
			}
			return nil
		}); err != nil {
			return err
		}
		if len(stale) == 0 {
			return errNothing
		}
		for _, k := range stale {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errNothing) {
		return fmt.Errorf("spool keep addresses: %w", err)
	}
	return nil
}
