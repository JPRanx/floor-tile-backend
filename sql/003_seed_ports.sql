-- Floor Tile SaaS - Seed Ports
-- Migration 003: Insert known ports

-- ORIGIN PORTS (Colombia)
INSERT INTO ports (name, country, type, unlocode) VALUES
('Cartagena', 'Colombia', 'ORIGIN', 'COCTG'),
('Barranquilla', 'Colombia', 'ORIGIN', 'COBAQ');

-- DESTINATION PORTS (Central America)
INSERT INTO ports (name, country, type, unlocode) VALUES
('Santo Tomas de Castilla', 'Guatemala', 'DESTINATION', 'GTSTC'),
('Puerto Quetzal', 'Guatemala', 'DESTINATION', 'GTPRQ'),
('Puerto Barrios', 'Guatemala', 'DESTINATION', 'GTPBR'),
('Puerto Cortes', 'Honduras', 'DESTINATION', 'HNPCR'),
('Acajutla', 'El Salvador', 'DESTINATION', 'SVAQJ');

-- Note: avg_processing_days will be learned from actual shipments
