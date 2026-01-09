CREATE TABLE IF NOT EXISTS tbl_sales (
    -- Identity & Keys
    sale_id             BIGSERIAL PRIMARY KEY,
    transaction_uuid    UUID NOT NULL DEFAULT gen_random_uuid(),
    
    -- Text & Categorical
    customer_name       VARCHAR(100) NOT NULL,
    sales_channel       VARCHAR(50) DEFAULT 'Direct', -- e.g., Online, Retail, Reseller
    region_code         CHAR(3), -- e.g., IDN, USA, SGP
    
    -- Temporal (Waktu)
    transaction_date    DATE NOT NULL,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    
    -- Boolean & Status
    is_vip_customer     BOOLEAN DEFAULT FALSE,
    is_refunded         BOOLEAN, -- Nullable (Unknown state allowed)
    
    -- Variasi Numeric (Scale & Precision)
    quantity            INTEGER NOT NULL CHECK (quantity > 0), -- Bilangan bulat
    
    unit_price          NUMERIC(12, 2) NOT NULL, -- Format mata uang standar (2 desimal)
    
    discount_pct        NUMERIC(5, 4), -- Presisi tinggi (e.g., 0.1255 untuk 12.55%)
    
    tax_amount          DOUBLE PRECISION, -- Floating point (approximate)
    
    shipping_weight_kg  REAL, -- Float yang lebih kecil
    
    exchange_rate       NUMERIC(18, 8), -- Presisi sangat tinggi (8 digit desimal)
    
    -- Advanced Types
    tags                TEXT[], -- Array of text (e.g., {'promo', 'flash_sale'})
    metadata            JSONB   -- Unstructured data (e.g., device info, browser)
);

-- Indexing untuk performa query (Opsional tapi recommended)
CREATE INDEX idx_sales_date ON tbl_sales(transaction_date);
CREATE INDEX idx_sales_metadata ON tbl_sales USING GIN (metadata);

INSERT INTO tbl_sales (
    customer_name, 
    sales_channel, 
    region_code, 
    transaction_date, 
    is_vip_customer, 
    is_refunded, 
    quantity, 
    unit_price, 
    discount_pct, 
    tax_amount, 
    shipping_weight_kg, 
    exchange_rate, 
    tags, 
    metadata
)
WITH generator AS (
    -- Ubah angka 1000 di bawah ini jika ingin jumlah baris yang berbeda
    SELECT generate_series(1, 1000) AS id
)
SELECT
    -- Generate Random Customer Name (ex: Customer-123)
    'Customer-' || (floor(random() * 10000)::text) AS customer_name,
    
    -- Random Sales Channel
    (ARRAY['Online', 'Retail', 'B2B', 'Affiliate'])[floor(random() * 4 + 1)] AS sales_channel,
    
    -- Random Region
    (ARRAY['IDN', 'SGP', 'USA', 'JPN'])[floor(random() * 4 + 1)] AS region_code,
    
    -- Random Date (within last 365 days)
    CURRENT_DATE - (floor(random() * 365) || ' days')::interval AS transaction_date,
    
    -- Random Boolean
    (random() > 0.8) AS is_vip_customer, -- 20% chance true
    
    -- Random Boolean with Nulls
    CASE 
        WHEN random() < 0.1 THEN NULL 
        WHEN random() < 0.2 THEN TRUE 
        ELSE FALSE 
    END AS is_refunded,
    
    -- Integer: 1 to 50
    floor(random() * 50 + 1)::int AS quantity,
    
    -- Numeric(12,2): Price between 10.00 and 10000.00
    ROUND((random() * 10000 + 10)::numeric, 2) AS unit_price,
    
    -- Numeric(5,4): Discount between 0.0000 and 0.5000
    ROUND((random() * 0.5)::numeric, 4) AS discount_pct,
    
    -- Double Precision
    (random() * 100) AS tax_amount,
    
    -- Real
    (random() * 20)::real AS shipping_weight_kg,
    
    -- Numeric(18,8): Exchange Rate example (e.g., IDR to USD style or Crypto)
    ROUND((random() * 15000 + 14000)::numeric, 8) AS exchange_rate,
    
    -- Array Text
    CASE 
        WHEN random() > 0.5 THEN ARRAY['flash_sale', 'electronic']
        ELSE ARRAY['regular', 'household']
    END AS tags,
    
    -- JSONB: Random device info
    jsonb_build_object(
        'device', (ARRAY['mobile', 'desktop', 'tablet'])[floor(random() * 3 + 1)],
        'os_version', floor(random() * 10 + 10),
        'click_id', md5(random()::text)
    ) AS metadata
FROM generator;