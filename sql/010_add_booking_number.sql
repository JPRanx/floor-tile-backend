-- Floor Tile SaaS - Database Migration
-- Migration 010: Add booking_number to shipments table
--
-- Purpose: Store freight forwarder booking reference (e.g., "BGA0505879")
-- Used for tracking order confirmations from CMA CGM and other carriers
--
-- Run this in Supabase SQL Editor

-- Add booking_number column (nullable text)
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS booking_number TEXT;

-- Create index for lookup by booking number
CREATE INDEX IF NOT EXISTS idx_shipments_booking_number ON shipments(booking_number);
