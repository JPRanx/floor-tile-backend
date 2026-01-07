-- Floor Tile SaaS - Database Migration
-- Migration 007: Rename sales column for consistency
--
-- Changes:
--   sold_at → week_start (matches model field name)
--
-- Run this in Supabase SQL Editor

-- Rename sold_at to week_start
ALTER TABLE sales
RENAME COLUMN sold_at TO week_start;

-- Update index name to match new column name
DROP INDEX IF EXISTS idx_sales_sold_at;
CREATE INDEX IF NOT EXISTS idx_sales_week_start ON sales(week_start DESC);
