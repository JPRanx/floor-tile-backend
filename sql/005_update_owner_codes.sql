-- Floor Tile SaaS - Update Owner Codes
-- Migration 005: Map owner's Excel SKU codes to products
-- Run in Supabase SQL Editor

-- First, add owner_code column if not exists:
ALTER TABLE products ADD COLUMN IF NOT EXISTS owner_code TEXT;
CREATE INDEX IF NOT EXISTS idx_products_owner_code ON products(owner_code);

-- Update all 31 product mappings:
UPDATE products SET owner_code = '102' WHERE sku = 'ALMENDRO BEIGE';
UPDATE products SET owner_code = '119' WHERE sku = 'ALMENDRO GRIS';
UPDATE products SET owner_code = '109' WHERE sku = 'BARANOA GRIS';
UPDATE products SET owner_code = '140' WHERE sku = 'CALARCA';
UPDATE products SET owner_code = '93' WHERE sku = 'CARACOLI';
UPDATE products SET owner_code = '99' WHERE sku = 'CEIBA BEIGE';
UPDATE products SET owner_code = '103' WHERE sku = 'CEIBA CAFÉ';
UPDATE products SET owner_code = '101' WHERE sku = 'CEIBA GRIS OSCURO BTE';
UPDATE products SET owner_code = '73' WHERE sku = 'CIRCASIA';
UPDATE products SET owner_code = '67' WHERE sku = 'GALERA RUSTICO GRIS';
UPDATE products SET owner_code = '80' WHERE sku = 'MALAMBO BEIGE';
UPDATE products SET owner_code = '112' WHERE sku = 'MALAMBO GRIS';
UPDATE products SET owner_code = '142' WHERE sku = 'MANAURE BEIGE';
UPDATE products SET owner_code = '134' WHERE sku = 'MANAURE GRIS';
UPDATE products SET owner_code = '131' WHERE sku = 'MIRACH';
UPDATE products SET owner_code = '116' WHERE sku = 'NECOCLI BEIGE';
UPDATE products SET owner_code = '117' WHERE sku = 'NECOCLI GRIS';
UPDATE products SET owner_code = '110' WHERE sku = 'NOGAL BEIGE';
UPDATE products SET owner_code = '98' WHERE sku = 'NOGAL CAFÉ';
UPDATE products SET owner_code = '94' WHERE sku = 'NOGAL GRIS OSC';
UPDATE products SET owner_code = '68' WHERE sku = 'PIJAO';
UPDATE products SET owner_code = '137' WHERE sku = 'QUIMBAYA BEIGE';
UPDATE products SET owner_code = '108' WHERE sku = 'QUIMBAYA GRIS';
UPDATE products SET owner_code = '163' WHERE sku = 'SALENTO GRIS';
UPDATE products SET owner_code = '104' WHERE sku = 'SAMAN BEIGE';
UPDATE products SET owner_code = '139' WHERE sku = 'SAMAN CAFÉ';
UPDATE products SET owner_code = '105' WHERE sku = 'SAMAN GRIS';
UPDATE products SET owner_code = '45' WHERE sku = 'TERRA FUERTE';
UPDATE products SET owner_code = '69' WHERE sku = 'TOLU BEIGE';
UPDATE products SET owner_code = '138' WHERE sku = 'TOLU CAFE';
UPDATE products SET owner_code = '22' WHERE sku = 'TOLU GRIS';

-- Verify: Check for any products without owner_code
-- (7 DB products not in owner's Excel: CEIBA GRIS OSC, MOMPOX BEIGE, MOMPOX CAFÉ, ROBLE BEIGE, ROBLE GRIS, MALAGA GRIS, MALAGA BEIGE)
SELECT sku, owner_code FROM products WHERE owner_code IS NULL ORDER BY sku;

-- Verify: All mapped products
SELECT sku, owner_code FROM products WHERE owner_code IS NOT NULL ORDER BY owner_code::int;
