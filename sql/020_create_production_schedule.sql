-- Floor Tile SaaS - Database Schema
-- Migration 020: Create Production Schedule Table
-- Run this in Supabase SQL Editor

-- ===================
-- PRODUCTION SCHEDULE
-- ===================
-- Tracks factory production requests and completions for Guatemala
-- Key insight: Items in 'scheduled' status can have more quantity added before production starts

CREATE TABLE production_schedule (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Product identification
    factory_item_code VARCHAR(50),                -- Factory's internal item code (e.g., 5549)
    product_id UUID REFERENCES products(id),     -- Linked product (nullable if not mapped)
    sku VARCHAR(50),                             -- Resolved SKU from our system
    referencia VARCHAR(255) NOT NULL,            -- Factory reference name (e.g., "SAMAN BEIGE BTE")

    -- Production data
    plant VARCHAR(20) NOT NULL,                  -- 'plant_1' or 'plant_2'
    requested_m2 DECIMAL(12,2) NOT NULL DEFAULT 0, -- Programa column (what was requested)
    completed_m2 DECIMAL(12,2) NOT NULL DEFAULT 0, -- Real column (what's done)

    -- Status (derived from Excel cell colors)
    -- scheduled = white (CAN ADD MORE before production starts)
    -- in_progress = blue (currently manufacturing)
    -- completed = green (finished)
    status VARCHAR(20) NOT NULL DEFAULT 'scheduled'
        CHECK (status IN ('scheduled', 'in_progress', 'completed')),

    -- Dates
    scheduled_start_date DATE,                   -- Fecha Inicio from Excel
    scheduled_end_date DATE,                     -- Fecha Fin from Excel
    estimated_delivery_date DATE,                -- Fecha estimada entrega
    actual_completion_date DATE,                 -- When marked completed

    -- Source tracking
    source_file VARCHAR(255),                    -- Original filename
    source_month VARCHAR(20),                    -- e.g., 'ENERO-26'
    source_row INT,                              -- Row number in Excel for debugging

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- ===================
-- INDEXES
-- ===================
CREATE INDEX idx_production_schedule_status ON production_schedule(status);
CREATE INDEX idx_production_schedule_product_id ON production_schedule(product_id);
CREATE INDEX idx_production_schedule_sku ON production_schedule(sku);
CREATE INDEX idx_production_schedule_referencia ON production_schedule(referencia);
CREATE INDEX idx_production_schedule_plant ON production_schedule(plant);

-- Unique constraint: same referencia + plant + source_month = same record
CREATE UNIQUE INDEX idx_production_schedule_unique
    ON production_schedule(referencia, plant, source_month);

-- ===================
-- TRIGGER FOR updated_at
-- ===================
CREATE OR REPLACE FUNCTION update_production_schedule_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER production_schedule_updated_at
    BEFORE UPDATE ON production_schedule
    FOR EACH ROW
    EXECUTE FUNCTION update_production_schedule_updated_at();

-- ===================
-- COMMENTS
-- ===================
COMMENT ON TABLE production_schedule IS 'Factory production schedule from Programa de Produccion Excel files';
COMMENT ON COLUMN production_schedule.status IS 'scheduled=white/can add more, in_progress=blue, completed=green';
COMMENT ON COLUMN production_schedule.requested_m2 IS 'Primera exportacion under Programa - what Guatemala requested';
COMMENT ON COLUMN production_schedule.completed_m2 IS 'Primera exportacion under Real - what factory completed';
COMMENT ON COLUMN production_schedule.referencia IS 'Product reference name from factory (e.g., SAMAN BEIGE BTE)';

-- ===================
-- USEFUL VIEWS
-- ===================

-- View: Products that can have more quantity added (scheduled, not started)
CREATE OR REPLACE VIEW v_production_can_add_more AS
SELECT
    ps.id,
    ps.referencia,
    ps.sku,
    ps.plant,
    ps.requested_m2,
    ps.completed_m2,
    ps.estimated_delivery_date,
    p.id as product_id,
    p.sku as matched_sku
FROM production_schedule ps
LEFT JOIN products p ON ps.product_id = p.id
WHERE ps.status = 'scheduled'
ORDER BY ps.referencia;

-- View: Production summary by status
CREATE OR REPLACE VIEW v_production_summary AS
SELECT
    status,
    COUNT(*) as item_count,
    SUM(requested_m2) as total_requested_m2,
    SUM(completed_m2) as total_completed_m2,
    CASE
        WHEN status = 'scheduled' THEN 'CAN ADD MORE'
        WHEN status = 'in_progress' THEN 'MANUFACTURING'
        WHEN status = 'completed' THEN 'READY TO SHIP'
    END as action_hint
FROM production_schedule
GROUP BY status
ORDER BY
    CASE status
        WHEN 'completed' THEN 1
        WHEN 'in_progress' THEN 2
        WHEN 'scheduled' THEN 3
    END;
