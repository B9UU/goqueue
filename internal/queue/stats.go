package queue

import (
	"context"
	"database/sql"
)

type jobCountRow struct {
	Queue  string `db:"queue"`
	Status string `db:"status"`
	Count  int    `db:"count"`
}

type dlqCountRow struct {
	Queue string `db:"queue"`
	Count int    `db:"count"`
}

func (s *PostgresStore) GetStats(ctx context.Context) (*Stats, error) {
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var jobRows []jobCountRow
	if err := tx.SelectContext(ctx, &jobRows, `
		SELECT queue, status::text, COUNT(*) AS count
		FROM jobs
		GROUP BY queue, status`); err != nil {
		return nil, err
	}

	var dlqRows []dlqCountRow
	if err := tx.SelectContext(ctx, &dlqRows, `
		SELECT queue, COUNT(*) AS count
		FROM dead_letter_jobs
		GROUP BY queue`); err != nil {
		return nil, err
	}

	stats := &Stats{
		Queues: make(map[string]QueueStats),
		DLQ:    make(map[string]int),
	}

	for _, row := range jobRows {
		qs := stats.Queues[row.Queue]
		switch Status(row.Status) {
		case StatusPending:
			qs.Pending = row.Count
		case StatusRunning:
			qs.Running = row.Count
		case StatusSucceeded:
			qs.Succeeded = row.Count
		case StatusFailed:
			qs.Failed = row.Count
		case StatusCancelled:
			qs.Cancelled = row.Count
		}
		stats.Queues[row.Queue] = qs
	}

	for _, row := range dlqRows {
		stats.DLQ[row.Queue] = row.Count
	}

	return stats, tx.Commit()
}
