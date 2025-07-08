-- Load set of input intervals from temp table
WITH input AS (
  -- Define range as exlusive to end_ts rounded up to the second to remove boundary gaps
  SELECT collection_id, range_agg(tsrange(start_ts, date_trunc('second', end_ts) + interval '1 second')) input_sets
  FROM input_records 
  GROUP BY collection_id
),

-- Existing gaps that intersect with input intervals are deleted
deleted_gaps AS (
    DELETE FROM gaps
    WHERE EXISTS (
        SELECT 1
        FROM input
        WHERE input.collection_id = gaps.collection_id
        -- Set intersection between current gaps and input intervals
        AND tsrange(start_ts, end_ts) && input.input_sets
    )
    RETURNING collection_id, tsrange(start_ts, end_ts) as target_gaps
),

-- Aggregate deleted gaps as a set
current_gaps AS (
    SELECT collection_id, range_agg(target_gaps) as gaps_set
    FROM deleted_gaps
    GROUP BY collection_id
),

new_gaps AS (
    SELECT c.collection_id, unnest(c.gaps_set - i.input_sets) as new_range
    FROM current_gaps c
    JOIN input i ON c.collection_id = i.collection_id
)

-- Replace gap intervals
INSERT INTO gaps (collection_id, start_ts, end_ts)
SELECT collection_id, lower(new_range), upper(new_range)
FROM new_gaps;
