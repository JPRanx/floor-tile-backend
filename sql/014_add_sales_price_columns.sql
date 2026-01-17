-- Floor Tile SaaS - Database Migration
-- Migration 014: Add pricing columns to sales table
--
-- Purpose: Enable revenue tracking in Analytics
-- - unit_price_usd: Sale price per m² in USD
-- - total_price_usd: Total sale value (quantity * unit_price)
--
-- Run this in Supabase SQL Editor

-- Add unit price column (price per m²)
ALTER TABLE sales ADD COLUMN IF NOT EXISTS unit_price_usd DECIMAL(10,2);

-- Add total price column (total sale value)
ALTER TABLE sales ADD COLUMN IF NOT EXISTS total_price_usd DECIMAL(10,2);

-- Index for revenue aggregation queries (partial index for efficiency)
CREATE INDEX IF NOT EXISTS idx_sales_total_price ON sales(total_price_usd) WHERE total_price_usd IS NOT NULL;
