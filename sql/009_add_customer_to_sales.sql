-- Floor Tile SaaS - Database Migration
-- Migration 009: Add customer columns to sales table
--
-- Purpose: Track customer data for confidence scoring
-- - customer: Original customer name (with accents, original casing)
-- - customer_normalized: Uppercase ASCII for grouping/comparison
--
-- Run this in Supabase SQL Editor

-- Add customer column (original name with accents preserved)
ALTER TABLE sales ADD COLUMN IF NOT EXISTS customer TEXT;

-- Add normalized customer column (for grouping/comparison)
ALTER TABLE sales ADD COLUMN IF NOT EXISTS customer_normalized TEXT;

-- Create index on normalized customer for efficient grouping
CREATE INDEX IF NOT EXISTS idx_sales_customer_normalized ON sales(customer_normalized);

-- Optional: Index on product_id + customer_normalized for customer analysis per product
CREATE INDEX IF NOT EXISTS idx_sales_product_customer ON sales(product_id, customer_normalized);
