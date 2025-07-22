CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE collections (
    collection_id VARCHAR(255) PRIMARY KEY,
    temporal_extent_start TIMESTAMP NOT NULL,
    temporal_extent_end TIMESTAMP,
    
    -- Ensure temporal extents are valid when present
    CHECK (temporal_extent_end IS NULL OR temporal_extent_start < temporal_extent_end)
);

CREATE INDEX idx_collections_temporal ON collections(temporal_extent_start, temporal_extent_end);

CREATE TABLE gaps (
    gap_id SERIAL,
    collection_id VARCHAR(255) NOT NULL,
    start_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP NOT NULL,
    reason VARCHAR(255),
    
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
CREATE INDEX idx_gaps_collection_id ON gaps(collection_id);
CREATE INDEX idx_gaps_temporal ON gaps(start_ts, end_ts);

-- Function that inserts initial gap when a collection is added
CREATE OR REPLACE FUNCTION init_gap()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO gaps (collection_id, start_ts, end_ts)
    VALUES (NEW.collection_id, NEW.temporal_extent_start, NEW.temporal_extent_end);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger that fires after a new collection is inserted
CREATE TRIGGER collection_insert_trigger
AFTER INSERT ON collections
FOR EACH ROW
EXECUTE FUNCTION init_gap();
