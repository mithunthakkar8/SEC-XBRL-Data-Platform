-- CREATE EXTENSION IF NOT EXISTS vector;

DROP INDEX xbrl.idx_hnsw_embeddings;
DROP TABLE xbrl.standard_label_embeddings;
CREATE TABLE xbrl.standard_label_embeddings (
    Standard_Label_ID UUID DEFAULT uuid_generate_v4() 
        CONSTRAINT pk_Standard_Label_ID PRIMARY KEY,
    standard_label TEXT NOT NULL,
    vector_embeddings vector(1024), -- Adjust dimension (e.g., 384, 512, 1024) based on DeepSeek's output
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insert data from your source table
INSERT INTO xbrl.standard_label_embeddings (standard_label)
SELECT distinct standard_label
FROM xbrl.label;

-- Add an index for fast similarity search
CREATE INDEX idx_hnsw_embeddings ON xbrl.standard_label_embeddings USING hnsw (vector_embeddings vector_l2_ops);