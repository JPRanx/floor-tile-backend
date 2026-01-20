-- Floor Tile SaaS - Seed Settings
-- Migration 004: Insert default settings

INSERT INTO settings (key, value) VALUES
-- Lead time and safety stock
('lead_time_days', '45'),
('safety_stock_service_level', '0.95'),
('safety_stock_z_score', '1.645'),

-- Container constraints
('container_max_pallets', '14'),
('container_max_weight_kg', '28000'),
('container_max_m2', '1881'),
('m2_per_pallet', '134.4'),

-- Boat constraints
('boat_min_containers', '3'),
('boat_max_containers', '5'),

-- Warehouse constraints
('warehouse_max_pallets', '740'),
('warehouse_max_m2', '100000'),
('warehouse_floor_m2', '900'),

-- Port timing
('origin_port_wait_days', '2'),
('destination_port_wait_days', '5'),

-- Pricing
('fob_price_cartagena_usd', '4.70'),

-- Alert thresholds
('stockout_critical_days', '14'),
('stockout_warning_days', '30'),
('free_days_critical', '2'),
('free_days_warning', '5'),

-- Unload time
('container_unload_hours', '2'),

-- Order cycle
('cycle_days', '45'),
('default_free_days', '14');