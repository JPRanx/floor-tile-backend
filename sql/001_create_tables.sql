-- Floor Tile SaaS - Database Schema
-- Migration 001: Create Tables
-- Run this in Supabase SQL Editor

-- ===================
-- PRODUCTS
-- ===================
CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL CHECK (category IN ('MADERAS', 'EXTERIORES', 'MARMOLIZADOS')),
    rotation TEXT CHECK (rotation IN ('ALTA', 'MEDIA-ALTA', 'MEDIA', 'BAJA')),
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_products_sku ON products(sku);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_rotation ON products(rotation);

-- ===================
-- INVENTORY SNAPSHOTS
-- ===================
CREATE TABLE inventory_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    warehouse_qty DECIMAL NOT NULL DEFAULT 0,
    factory_qty DECIMAL NOT NULL DEFAULT 0,
    counted_at DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_inventory_product ON inventory_snapshots(product_id);
CREATE INDEX idx_inventory_counted_at ON inventory_snapshots(counted_at DESC);

-- ===================
-- SALES
-- ===================
CREATE TABLE sales (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    quantity DECIMAL NOT NULL,
    sold_at DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_sales_product ON sales(product_id);
CREATE INDEX idx_sales_sold_at ON sales(sold_at DESC);

-- ===================
-- FACTORY AVAILABILITY
-- ===================
CREATE TABLE factory_availability (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    factory_item_code TEXT,
    quantity DECIMAL NOT NULL,
    production_start DATE,
    production_end DATE,
    estimated_port_ready DATE,
    report_date DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_factory_availability_product ON factory_availability(product_id);
CREATE INDEX idx_factory_availability_report_date ON factory_availability(report_date DESC);

-- ===================
-- FACTORY ORDERS
-- ===================
CREATE TABLE factory_orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_date DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'CONFIRMED', 'IN_PRODUCTION', 'READY', 'SHIPPED')),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_factory_orders_status ON factory_orders(status);
CREATE INDEX idx_factory_orders_date ON factory_orders(order_date DESC);

-- ===================
-- FACTORY ORDER ITEMS
-- ===================
CREATE TABLE factory_order_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    factory_order_id UUID NOT NULL REFERENCES factory_orders(id) ON DELETE CASCADE,
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    quantity_ordered DECIMAL NOT NULL,
    quantity_produced DECIMAL DEFAULT 0,
    estimated_ready_date DATE,
    actual_ready_date DATE,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_factory_order_items_order ON factory_order_items(factory_order_id);
CREATE INDEX idx_factory_order_items_product ON factory_order_items(product_id);

-- ===================
-- PORTS
-- ===================
CREATE TABLE ports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    country TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('ORIGIN', 'DESTINATION')),
    unlocode TEXT,
    avg_processing_days DECIMAL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_ports_type ON ports(type);

-- ===================
-- SHIPPING COMPANIES
-- ===================
CREATE TABLE shipping_companies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    free_days_at_destination INTEGER,
    avg_transit_days DECIMAL,
    reliability_score DECIMAL,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- ===================
-- TRUCKING COMPANIES
-- ===================
CREATE TABLE trucking_companies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    country TEXT NOT NULL,
    ports_covered TEXT[],
    avg_cost_usd DECIMAL,
    on_time_percentage DECIMAL,
    total_deliveries INTEGER DEFAULT 0,
    incidents INTEGER DEFAULT 0,
    reliability_score DECIMAL,
    contact_name TEXT,
    contact_phone TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_trucking_country ON trucking_companies(country);

-- ===================
-- SHIPMENTS
-- ===================
CREATE TABLE shipments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shipping_company_id UUID REFERENCES shipping_companies(id),
    origin_port_id UUID NOT NULL REFERENCES ports(id),
    destination_port_id UUID NOT NULL REFERENCES ports(id),
    status TEXT NOT NULL DEFAULT 'AT_FACTORY' CHECK (status IN ('AT_FACTORY', 'AT_ORIGIN_PORT', 'IN_TRANSIT', 'AT_DESTINATION_PORT', 'IN_CUSTOMS', 'IN_TRUCK', 'DELIVERED')),
    vessel_name TEXT,
    voyage_number TEXT,
    bill_of_lading TEXT,
    etd DATE,
    eta DATE,
    actual_departure DATE,
    actual_arrival DATE,
    free_days INTEGER,
    free_days_expiry DATE,
    freight_cost_usd DECIMAL,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_shipments_status ON shipments(status);
CREATE INDEX idx_shipments_eta ON shipments(eta);

-- ===================
-- CONTAINERS
-- ===================
CREATE TABLE containers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shipment_id UUID NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
    container_number TEXT,
    seal_number TEXT,
    trucking_company_id UUID REFERENCES trucking_companies(id),
    total_pallets INTEGER,
    total_weight_kg DECIMAL,
    total_m2 DECIMAL,
    fill_percentage DECIMAL,
    unload_start TIMESTAMPTZ,
    unload_end TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_containers_shipment ON containers(shipment_id);
CREATE INDEX idx_containers_number ON containers(container_number);

-- ===================
-- CONTAINER ITEMS
-- ===================
CREATE TABLE container_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    container_id UUID NOT NULL REFERENCES containers(id) ON DELETE CASCADE,
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    quantity DECIMAL NOT NULL,
    pallets INTEGER,
    weight_kg DECIMAL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_container_items_container ON container_items(container_id);
CREATE INDEX idx_container_items_product ON container_items(product_id);

-- ===================
-- SHIPMENT EVENTS
-- ===================
CREATE TABLE shipment_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shipment_id UUID NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_shipment_events_shipment ON shipment_events(shipment_id);
CREATE INDEX idx_shipment_events_occurred ON shipment_events(occurred_at DESC);

-- ===================
-- ALERTS
-- ===================
CREATE TABLE alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type TEXT NOT NULL CHECK (type IN ('STOCKOUT_WARNING', 'LOW_STOCK', 'ORDER_OPPORTUNITY', 'SHIPMENT_DEPARTED', 'SHIPMENT_ARRIVED', 'FREE_DAYS_EXPIRING', 'SHIPMENT_DELAYED', 'CONTAINER_READY', 'OVER_STOCKED')),
    severity TEXT NOT NULL CHECK (severity IN ('CRITICAL', 'WARNING', 'INFO')),
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    product_id UUID REFERENCES products(id) ON DELETE SET NULL,
    shipment_id UUID REFERENCES shipments(id) ON DELETE SET NULL,
    is_read BOOLEAN DEFAULT false,
    is_sent BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_alerts_severity ON alerts(severity);
CREATE INDEX idx_alerts_is_read ON alerts(is_read);
CREATE INDEX idx_alerts_created ON alerts(created_at DESC);

-- ===================
-- SETTINGS
-- ===================
CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- ===================
-- UPDATED_AT TRIGGER
-- ===================
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER factory_orders_updated_at
    BEFORE UPDATE ON factory_orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER shipments_updated_at
    BEFORE UPDATE ON shipments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER settings_updated_at
    BEFORE UPDATE ON settings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
