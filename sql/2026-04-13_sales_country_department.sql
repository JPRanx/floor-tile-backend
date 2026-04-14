-- Add country + department columns to sales table
-- Source: PAIS and DEPARTAMENTO columns from Tarragona SAC sales file
-- Pre-existing country inference in trend_service.py remains as fallback
-- for historical rows until they're re-uploaded.

ALTER TABLE sales
  ADD COLUMN IF NOT EXISTS country TEXT,
  ADD COLUMN IF NOT EXISTS department TEXT;

CREATE INDEX IF NOT EXISTS idx_sales_country ON sales(country) WHERE country IS NOT NULL;
