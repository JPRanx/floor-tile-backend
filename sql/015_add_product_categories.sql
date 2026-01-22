-- Migration: Add new product categories for non-tile products
-- Date: 2026-01-22

-- Drop the existing category constraint
ALTER TABLE products DROP CONSTRAINT IF EXISTS products_category_check;

-- Add new constraint with expanded categories
ALTER TABLE products ADD CONSTRAINT products_category_check
    CHECK (category IN ('MADERAS', 'EXTERIORES', 'MARMOLIZADOS', 'FURNITURE', 'SINK', 'SURCHARGE', 'OTHER'));

-- Add bathroom furniture products
INSERT INTO products (sku, category, rotation, active) VALUES
    ('MUEBLE DE BANO MUSTANG EXP. WENGUE', 'FURNITURE', 'BAJA', true),
    ('MUEBLE DE BANO MUSTANG EXP. GERMANY', 'FURNITURE', 'BAJA', true),
    ('MUEBLE DE BANO MUSTANG EXP. BLANCO', 'FURNITURE', 'BAJA', true),
    ('MUEBLE DE BANO CORN EXP. WENGUE', 'FURNITURE', 'BAJA', true),
    ('MUEBLE DE BANO CORN EXP. GERMANY', 'FURNITURE', 'BAJA', true),
    ('MUEBLE DE BANO CORN EXP. BLANCO', 'FURNITURE', 'BAJA', true),
    ('MUEBLE DE BANO ROSSO EXP. WENGUE', 'FURNITURE', 'BAJA', true),
    ('MUEBLE DE BANO ROSSO EXP. GERMANY', 'FURNITURE', 'BAJA', true),
    ('MUEBLE DE BANO ROSSO EXP. BLANCO', 'FURNITURE', 'BAJA', true);

-- Add sink products
INSERT INTO products (sku, category, rotation, active) VALUES
    ('LAVAMANOS DE CERAMICA PARA GABINETE', 'SINK', 'BAJA', true);

-- Add surcharge products (for tracking purposes)
INSERT INTO products (sku, category, rotation, active) VALUES
    ('RECARGOS EXTERIOR', 'SURCHARGE', 'BAJA', true);
