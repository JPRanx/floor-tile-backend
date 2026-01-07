-- Floor Tile SaaS - Database Schema
-- Migration 008: Create Boat Schedules Table
-- Run this in Supabase SQL Editor

-- ===================
-- BOAT SCHEDULES
-- ===================
-- Tracks shipping vessel schedules from origin ports to Guatemala
-- Used for order planning and booking deadlines

CREATE TABLE boat_schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vessel_name VARCHAR(100),
    shipping_line VARCHAR(100),
    departure_date DATE NOT NULL,
    arrival_date DATE NOT NULL,
    transit_days INTEGER NOT NULL,
    origin_port VARCHAR(100) NOT NULL DEFAULT 'Castellon',
    destination_port VARCHAR(100) NOT NULL DEFAULT 'Puerto Quetzal',
    route_type VARCHAR(50) CHECK (route_type IN ('direct', 'with_stops')),
    booking_deadline DATE NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'available'
        CHECK (status IN ('available', 'booked', 'departed', 'arrived')),
    source_file VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Indexes for common queries
CREATE INDEX idx_boat_schedules_departure ON boat_schedules(departure_date);
CREATE INDEX idx_boat_schedules_status ON boat_schedules(status);
CREATE INDEX idx_boat_schedules_booking_deadline ON boat_schedules(booking_deadline);

-- Unique constraint to prevent duplicate entries
-- Same vessel on same departure date is the same schedule
CREATE UNIQUE INDEX idx_boat_schedules_unique
    ON boat_schedules(departure_date, COALESCE(vessel_name, ''));

-- Trigger for updated_at
CREATE OR REPLACE FUNCTION update_boat_schedules_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER boat_schedules_updated_at
    BEFORE UPDATE ON boat_schedules
    FOR EACH ROW
    EXECUTE FUNCTION update_boat_schedules_updated_at();

-- ===================
-- COMMENTS
-- ===================
COMMENT ON TABLE boat_schedules IS 'Shipping vessel schedules for order planning';
COMMENT ON COLUMN boat_schedules.booking_deadline IS 'Last date to book cargo on this vessel (departure - 3 days)';
COMMENT ON COLUMN boat_schedules.route_type IS 'direct = no stops, with_stops = has intermediate ports';
COMMENT ON COLUMN boat_schedules.source_file IS 'Original filename from TIBA Excel upload';
