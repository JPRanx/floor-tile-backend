-- Migration: Create pending_documents table
-- Purpose: Store unmatched documents for later resolution

CREATE TABLE IF NOT EXISTS pending_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- Document info
    document_type TEXT NOT NULL,           -- 'hbl', 'mbl', 'booking', etc.
    parsed_data JSONB NOT NULL,            -- Full parsed result from Claude
    pdf_storage_path TEXT NOT NULL,        -- Supabase Storage path

    -- Source info
    source TEXT NOT NULL,                  -- 'email' or 'manual'
    email_subject TEXT,
    email_from TEXT,

    -- Matching context (what we tried)
    attempted_booking TEXT,
    attempted_shp TEXT,
    attempted_containers TEXT[],

    -- Resolution
    status TEXT DEFAULT 'pending',         -- 'pending', 'resolved', 'expired'
    resolved_at TIMESTAMPTZ,
    resolved_shipment_id UUID REFERENCES shipments(id),
    resolved_action TEXT,                  -- 'assigned', 'created', 'discarded'

    -- Housekeeping
    expires_at TIMESTAMPTZ DEFAULT NOW() + INTERVAL '30 days'
);

-- Index for listing pending documents
CREATE INDEX IF NOT EXISTS idx_pending_docs_status ON pending_documents(status);

-- Index for expiration cleanup
CREATE INDEX IF NOT EXISTS idx_pending_docs_expires ON pending_documents(expires_at) WHERE status = 'pending';

COMMENT ON TABLE pending_documents IS 'Queue for unmatched shipping documents awaiting manual resolution';
COMMENT ON COLUMN pending_documents.parsed_data IS 'Full ParsedDocumentData as JSON from Claude Vision parsing';
COMMENT ON COLUMN pending_documents.pdf_storage_path IS 'Path in Supabase Storage bucket: documents/pending/...';
