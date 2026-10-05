-- ============================================================================
-- BUILDING MATERIALS SALES - DATABASE SCHEMA
-- Architecture: Star Schema (Hybrid 3NF for dims, Denormalized fact)
-- 
-- Design Rationale:
-- - Dimension tables: 3NF (no redundancy, easy to maintain)
-- - Fact table: Denormalized (fast analytical queries)
-- - Macro data: Separate dimension table (linked by date)
-- ============================================================================

-- ============================================================================
-- DIMENSION TABLES (3NF)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- dim_date: Date dimension with calendar attributes
-- Contains 313 weeks (2019-01-07 to 2024-12-30)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL CHECK (month BETWEEN 1 AND 12),
    week_of_year INT NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    quarter INT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    day_of_week INT NOT NULL,
    day_name VARCHAR(10),
    month_name VARCHAR(10),
    is_holiday BOOLEAN DEFAULT FALSE,
    is_weekend BOOLEAN DEFAULT FALSE,
    fiscal_year INT,
    
    CONSTRAINT uq_dim_date_date UNIQUE (date)
);

-- ----------------------------------------------------------------------------
-- dim_region: US Census Division regions
-- Contains 9 unique regions
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_region (
    region_id SERIAL PRIMARY KEY,
    region_name VARCHAR(50) NOT NULL UNIQUE,
    census_division VARCHAR(50) NOT NULL,
    region_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- dim_category: Product categories
-- Contains 10 categories
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_category (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL UNIQUE,
    category_code VARCHAR(10),
    category_group VARCHAR(50),
    is_lumber_sensitive BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- dim_channel: Sales channels
-- Contains 2 channels: pro_dealer, big_box_diy
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_channel (
    channel_id SERIAL PRIMARY KEY,
    channel_name VARCHAR(50) NOT NULL UNIQUE,
    channel_type VARCHAR(50),
    channel_code VARCHAR(10),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- dim_customer: Customer types/segments
-- Contains 4 customer types
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_type VARCHAR(50) NOT NULL UNIQUE,
    segment VARCHAR(50),
    customer_code VARCHAR(10),
    description TEXT,
    is_business BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- dim_macro: Macroeconomic indicators by week
-- Contains 313 weeks with external economic data
-- Note: season_factor included here (was missing from transactions!)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_macro (
    macro_id SERIAL PRIMARY KEY,
    week DATE NOT NULL UNIQUE,
    housing_starts_index DECIMAL(6, 2),
    lumber_price_index DECIMAL(8, 2),
    mortgage_rate DECIMAL(5, 3),
    season_factor DECIMAL(5, 3),
    -- Composite indices
    housing_momentum DECIMAL(6, 2),
    lumber_volatility DECIMAL(6, 2),
    -- Metadata
    data_source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_dim_macro_week UNIQUE (week)
);

-- ----------------------------------------------------------------------------
-- dim_product: Product catalog with SKU
-- Contains ~84,641 unique SKUs
-- Note: SKU prefix encodes category (e.g., ROO-1234 -> roofing)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(50) NOT NULL UNIQUE,
    sku_prefix VARCHAR(10),
    category_id INT REFERENCES dim_category(category_id),
    product_name VARCHAR(255),
    description TEXT,
    base_price DECIMAL(10, 2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- fact_sales: Sales transactions fact table
-- Contains 340,000 transaction rows
-- Denormalized for fast analytical queries
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_sales (
    -- Surrogate primary key
    transaction_id BIGSERIAL PRIMARY KEY,
    
    -- Foreign keys to dimensions
    date_id INT NOT NULL REFERENCES dim_date(date_id),
    product_id INT NOT NULL REFERENCES dim_product(product_id),
    region_id INT NOT NULL REFERENCES dim_region(region_id),
    channel_id INT NOT NULL REFERENCES dim_channel(channel_id),
    customer_id INT NOT NULL REFERENCES dim_customer(customer_id),
    macro_id INT REFERENCES dim_macro(macro_id),
    
    -- Degenerate dimensions (useful attributes from dimensions)
    fiscal_year INT,
    quarter INT,
    
    -- Measureable facts
    units INT NOT NULL CHECK (units >= 0),
    unit_price DECIMAL(12, 4) NOT NULL CHECK (unit_price > 0),
    revenue DECIMAL(15, 2) NOT NULL CHECK (revenue >= 0),
    
    -- Inline macro indicators (denormalized for query performance)
    -- Alternative: join via macro_id, but this avoids JOIN for common queries
    housing_starts_index DECIMAL(6, 2),
    lumber_price_index DECIMAL(8, 2),
    mortgage_rate DECIMAL(5, 3),
    
    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint for UPSERT operations
    CONSTRAINT uq_fact_sales_business_key 
        UNIQUE (date_id, region_id, product_id, channel_id, customer_id)
);

CREATE TABLE IF NOT EXISTS etl_errors (
    error_id SERIAL PRIMARY KEY,
    source_file VARCHAR(255),
    source_row_number INT,
    raw_data JSONB,
    error_type VARCHAR(50),
    error_message TEXT,
    validation_rule VARCHAR(100),
    rejected_value TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'PENDING'
);

CREATE TABLE IF NOT EXISTS etl_batch_log (
    batch_id SERIAL PRIMARY KEY,
    batch_name VARCHAR(100),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20),
    rows_processed INT DEFAULT 0,
    rows_inserted INT DEFAULT 0,
    rows_updated INT DEFAULT 0,
    rows_rejected INT DEFAULT 0,
    error_count INT DEFAULT 0,
    metadata JSONB
);

CREATE TABLE IF NOT EXISTS etl_watermarks (
    source_name VARCHAR(100) PRIMARY KEY,
    last_processed_value VARCHAR(255),
    last_processed_at TIMESTAMP,
    batch_id INT REFERENCES etl_batch_log(batch_id)
);

CREATE OR REPLACE VIEW v_weekly_sales AS
SELECT 
    d.date,
    d.year,
    d.month,
    d.quarter,
    r.region_name,
    c.category_name,
    p.sku,
    ch.channel_name,
    cu.customer_type,
    m.housing_starts_index,
    m.lumber_price_index,
    m.mortgage_rate,
    m.season_factor,
    SUM(f.units) AS total_units,
    SUM(f.revenue) AS total_revenue,
    AVG(f.unit_price) AS avg_unit_price,
    COUNT(*) AS transaction_count
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_category c ON p.category_id = c.category_id
JOIN dim_region r ON f.region_id = r.region_id
JOIN dim_channel ch ON f.channel_id = ch.channel_id
JOIN dim_customer cu ON f.customer_id = cu.customer_id
LEFT JOIN dim_macro m ON f.macro_id = m.macro_id
GROUP BY d.date, d.year, d.month, d.quarter, 
         r.region_name, c.category_name, p.sku,
         ch.channel_name, cu.customer_type,
         m.housing_starts_index, m.lumber_price_index, 
         m.mortgage_rate, m.season_factor;

-- Summary by category view
CREATE OR REPLACE VIEW v_sales_by_category AS
SELECT 
    d.year,
    d.month,
    c.category_id,
    c.category_name,
    c.is_lumber_sensitive,
    SUM(f.units) AS total_units,
    SUM(f.revenue) AS total_revenue,
    AVG(f.unit_price) AS avg_unit_price,
    COUNT(DISTINCT p.sku) AS unique_skus
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_category c ON p.category_id = c.category_id
GROUP BY d.year, d.month, c.category_id, c.category_name, c.is_lumber_sensitive;

-- Macro indicators impact analysis view
CREATE OR REPLACE VIEW v_macro_impact AS
SELECT 
    m.week,
    m.housing_starts_index,
    m.lumber_price_index,
    m.mortgage_rate,
    m.season_factor,
    SUM(f.revenue) AS weekly_revenue,
    SUM(f.units) AS weekly_units
FROM dim_macro m
LEFT JOIN fact_sales f ON m.week = (SELECT date FROM dim_date WHERE date_id = f.date_id)
GROUP BY m.week, m.housing_starts_index, m.lumber_price_index, 
         m.mortgage_rate, m.season_factor;

-- ============================================================================
-- COMMENTS (Documentation)
-- ============================================================================

COMMENT ON TABLE dim_date IS 'Date dimension: 313 weeks from 2019-01-07 to 2024-12-30';
COMMENT ON TABLE dim_region IS 'US Census Division regions: 9 unique regions';
COMMENT ON TABLE dim_category IS 'Product categories: 10 building material categories';
COMMENT ON TABLE dim_product IS 'Product catalog: ~84,641 unique SKUs with prefix encoding';
COMMENT ON TABLE dim_channel IS 'Sales channels: pro_dealer and big_box_diy';
COMMENT ON TABLE dim_customer IS 'Customer segments: homebuilder, contractor, remodeler, homeowner_diy';
COMMENT ON TABLE dim_macro IS 'Macroeconomic indicators: housing starts, lumber price, mortgage rate, season factor';
COMMENT ON TABLE fact_sales IS 'Sales transactions: 340K rows, denormalized for analytics';
COMMENT ON TABLE etl_errors IS 'Error handling: dead letter queue for invalid records';

COMMENT ON COLUMN dim_product.sku_prefix IS 'First 3 chars of SKU encode category (e.g., ROO=lumber, LUM=roofing)';
COMMENT ON COLUMN dim_category.is_lumber_sensitive IS 'Flag for categories affected by lumber price volatility';
COMMENT ON COLUMN fact_sales.macro_id IS 'Optional link to macro indicators; embedded for query performance';
COMMENT ON COLUMN fact_sales.revenue IS 'Computed as units * unit_price; can be verified against source';