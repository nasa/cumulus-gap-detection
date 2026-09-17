-- Round granule end time up to nearest second to eliminate boundary gaps
WITH input_ranges AS (
    SELECT collection_id, tsrange(start_ts, date_trunc('second', end_ts) + interval '1 second') as gap_range
    FROM input_records
),

-- Remove adjacent existing gaps
removed_gaps AS (
    DELETE FROM gaps WHERE collection_id = %(collection_id)s AND gap_id IN (
        SELECT gap_id FROM gaps, input_ranges 
        WHERE gaps.collection_id = %(collection_id)s
        AND (tsrange(gaps.start_ts, gaps.end_ts) && input_ranges.gap_range 
             OR tsrange(gaps.start_ts, gaps.end_ts) -|- input_ranges.gap_range)
    ) RETURNING collection_id, tsrange(start_ts, end_ts) as gap_range
),
-- Merge new gaps with existing gaps
all_ranges AS (
    SELECT collection_id, gap_range FROM input_ranges
    UNION ALL SELECT collection_id, gap_range FROM removed_gaps
)
INSERT INTO gaps (collection_id, start_ts, end_ts)
SELECT collection_id, lower(merged_range), upper(merged_range) 
FROM (SELECT collection_id, unnest(range_agg(gap_range)) merged_range 
      FROM all_ranges GROUP BY collection_id) final_ranges
