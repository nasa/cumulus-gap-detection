CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS collections (
    collection_id VARCHAR(255) PRIMARY KEY,
    temporal_extent_start TIMESTAMP NOT NULL,
    temporal_extent_end TIMESTAMP,
    
    -- Ensure temporal extents are valid 
    CHECK (temporal_extent_end IS NULL OR temporal_extent_start < temporal_extent_end)
);

CREATE INDEX IF NOT EXISTS idx_collections_temporal ON collections(temporal_extent_start, temporal_extent_end);

CREATE TABLE IF NOT EXISTS gaps (
    gap_id SERIAL,
    collection_id VARCHAR(255) NOT NULL,
    start_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP NOT NULL,
    
    -- Basic checks on timestamps
    CHECK (start_ts < end_ts),
    CHECK (start_ts != end_ts),

    -- Unique constraint 
    CONSTRAINT no_duplicate_intervals
        UNIQUE (collection_id, start_ts, end_ts),

    -- Foreign key to collections table
    CONSTRAINT fk_collection
        FOREIGN KEY (collection_id)
        REFERENCES collections(collection_id)
        ON DELETE CASCADE

) PARTITION BY LIST (collection_id);

-- Create indices for common gap queries
CREATE INDEX IF NOT EXISTS idx_gaps_collection_id ON gaps(collection_id);
CREATE INDEX IF NOT EXISTS idx_gaps_temporal ON gaps(start_ts, end_ts);

-- Function that inserts initial gap when a collection is added
CREATE OR REPLACE FUNCTION init_gap()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO gaps (collection_id, start_ts, end_ts)
    VALUES (NEW.collection_id, NEW.temporal_extent_start, NEW.temporal_extent_end);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger init_gap function on collection insertion
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'collection_insert_trigger') THEN
        CREATE TRIGGER collection_insert_trigger
        AFTER INSERT ON collections
        FOR EACH ROW
        EXECUTE FUNCTION init_gap();
    END IF;
END $$;

-- Create reasons table
CREATE TABLE IF NOT EXISTS reasons (
    collection_id VARCHAR(255) NOT NULL,
    start_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP NOT NULL,
    reason VARCHAR(255) NOT NULL,
    
    CHECK (start_ts < end_ts),
    CHECK (start_ts != end_ts),
    CHECK (end_ts - start_ts >= INTERVAL '1 second'),
    
    PRIMARY KEY (collection_id, start_ts, end_ts)
) PARTITION BY LIST (collection_id);

CREATE INDEX IF NOT EXISTS idx_gap_reasons_collection_id ON reasons(collection_id);
CREATE INDEX IF NOT EXISTS idx_gap_reasons_temporal ON reasons(start_ts, end_ts);

-- Remove reason column from gaps table if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'gaps' AND column_name = 'reason') THEN
        ALTER TABLE gaps DROP COLUMN reason;
    END IF;
END $$;
