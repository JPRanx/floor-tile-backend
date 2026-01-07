-- Floor Tile SaaS - Database Migration
-- Migration 006: Rename inventory_snapshots columns for clarity
--
-- Changes:
--   factory_qty → in_transit_qty (owner tracks in-transit, not factory)
--   counted_at → snapshot_date (matches model field name)
--
-- Run this in Supabase SQL Editor

-- Check current columns first:
-- SELECT column_name FROM information_schema.columns
-- WHERE table_name = 'inventory_snapshots';

-- Rename factory_qty to in_transit_qty (skip if already renamed)
-- This column stores "En Tránsito (m²)" from owner's Excel - inventory on ships
ALTER TABLE inventory_snapshots
RENAME COLUMN factory_qty TO in_transit_qty;

-- Rename counted_at to snapshot_date (skip if already renamed)
-- Matches the model field name for consistency
ALTER TABLE inventory_snapshots
RENAME COLUMN counted_at TO snapshot_date;

-- Update index name to match new column name
DROP INDEX IF EXISTS idx_inventory_counted_at;
CREATE INDEX IF NOT EXISTS idx_inventory_snapshot_date ON inventory_snapshots(snapshot_date DESC);
