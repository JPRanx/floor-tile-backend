-- Floor Tile SaaS - Database Migration
-- Migration 011: Add order-shipment linking fields
--
-- Purpose: Connect factory orders to shipments for full order lifecycle tracking
-- - pv_number: Pedido de Ventas reference (e.g., "PV-00017759")
-- - factory_order_id: Link shipment back to the factory order
-- - shp_number: TIBA shipment reference (e.g., "SHP0065011")
-- - boat_schedule_id: Link to planned boat (optional, for planning)
--
-- Run this in Supabase SQL Editor

-- ===================
-- FACTORY ORDERS
-- ===================

-- Add PV number (Pedido de Ventas reference)
ALTER TABLE factory_orders ADD COLUMN IF NOT EXISTS pv_number TEXT;
CREATE INDEX IF NOT EXISTS idx_factory_orders_pv ON factory_orders(pv_number);

-- ===================
-- SHIPMENTS
-- ===================

-- Add factory_order link (connects shipment to its originating order)
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS factory_order_id UUID REFERENCES factory_orders(id);
CREATE INDEX IF NOT EXISTS idx_shipments_factory_order ON shipments(factory_order_id);

-- Add TIBA SHP number (TIBA's internal shipment reference)
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS shp_number TEXT;
CREATE INDEX IF NOT EXISTS idx_shipments_shp ON shipments(shp_number);

-- Add boat_schedule link (optional: links to planned vessel from TIBA booking table)
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS boat_schedule_id UUID REFERENCES boat_schedules(id);
CREATE INDEX IF NOT EXISTS idx_shipments_boat_schedule ON shipments(boat_schedule_id);
