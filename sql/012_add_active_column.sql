-- Floor Tile SaaS - Database Migration
-- Migration 012: Add active column for soft delete
--
-- Purpose: Enable soft delete on factory_orders and shipments tables
-- Used by the new Phase 2 services for CRUD operations
--
-- Run this in Supabase SQL Editor

-- ===================
-- FACTORY ORDERS
-- ===================
ALTER TABLE factory_orders ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT true;
CREATE INDEX IF NOT EXISTS idx_factory_orders_active ON factory_orders(active);

-- ===================
-- SHIPMENTS
-- ===================
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT true;
CREATE INDEX IF NOT EXISTS idx_shipments_active ON shipments(active);