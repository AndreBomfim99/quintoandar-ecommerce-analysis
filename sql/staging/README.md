# Staging Layer - Olist E-commerce Analysis

## Overview
The staging layer contains cleaned and standardized tables from the raw Olist Brazilian e-commerce dataset. All transformations follow data engineering best practices: deduplication, validation, standardization, and enrichment.

---

## 📊 Staging Tables

### Execution Order
Due to foreign key validations, tables must be created in this specific order:

1. **stg_customers** (no dependencies)
2. **stg_sellers** (no dependencies)
3. **stg_products** (no dependencies)
4. **stg_geolocation** (no dependencies)
5. **stg_orders** (validates FK to customers)
6. **stg_order_items** (validates FK to orders, products, sellers)
7. **stg_payments** (validates FK to orders)
8. **stg_reviews** (validates FK to orders)

---

## 1. stg_customers

**Source:** `olist_raw.customers`  
**Primary Key:** `customer_id`

### Main Transformations
- ✅ Deduplication by `customer_id`
- ✅ Standardized location data (uppercase state, title case city)
- ✅ Validated Brazilian states (27 UFs)
- ✅ Validated ZIP codes (5-digit format, valid range)
- ✅ **Enrichment:** Added `customer_region` (Norte, Nordeste, Centro-Oeste, Sudeste, Sul)

### Data Quality
- **Before:** ~99,441 rows
- **After:** ~96,096 rows (unique customers)
- **Null handling:** Removed rows with missing IDs or invalid states

### Output Columns
```
customer_id
customer_unique_id
customer_zip_code_prefix
customer_city
customer_state
customer_region (created)
```

---

## 2. stg_sellers

**Source:** `olist_raw.sellers`  
**Primary Key:** `seller_id`

### Main Transformations
- ✅ Deduplication by `seller_id`
- ✅ Standardized location data (uppercase state, title case city)
- ✅ Validated Brazilian states (27 UFs)
- ✅ Validated ZIP codes (5-digit format, valid range)
- ✅ **Enrichment:** Added `seller_region`

### Data Quality
- **Before:** ~3,095 rows
- **After:** ~3,095 rows (unique sellers)
- **Coverage:** ~70% concentrated in Southeast region

### Output Columns
```
seller_id
seller_zip_code_prefix
seller_city
seller_state
seller_region (created)
```

---

## 3. stg_products

**Source:** `olist_raw.products` + `olist_raw.category_translation`  
**Primary Key:** `product_id`

### Main Transformations
- ✅ Deduplication by `product_id`
- ✅ **JOIN:** Translated categories from Portuguese to English
- ✅ Validated dimensions (weight, length, height, width)
- ✅ Removed outliers (products > 100kg or > 3m)
- ✅ Handled missing categories as 'unknown'
- ⚠️ **Note:** Preserved original column names with typos (`product_name_lenght`, `product_description_lenght`)

### Data Quality
- **Before:** ~32,951 rows
- **After:** ~32,951 rows (unique products)
- **Categories:** ~70 categories translated
- **Missing dimensions:** ~10% of products lack complete dimension data

### Output Columns
```
product_id
product_category_name (original Portuguese)
product_category_name_english (translated)
product_name_lenght
product_description_lenght
product_photos_qty
product_weight_g (validated)
product_length_cm (validated)
product_height_cm (validated)
product_width_cm (validated)
```

---

## 4. stg_geolocation (ENRICHED)

**Source:** `olist_raw.geolocation` + `basedosdados.br_bd_diretorios_brasil.cep`  
**Primary Key:** `geolocation_zip_code_prefix`

### Main Transformations
- ✅ **ENRICHMENT:** LEFT JOIN with Base dos Dados (official Brazilian data)
- ✅ Prioritized official city names from Base dos Dados
- ✅ Extracted coordinates from `centroide` (GEOGRAPHY type)
- ✅ Validated coordinates (Brazil bounds: lat -34 to 5, lng -74 to -34)
- ✅ Removed invalid coordinates (0, 0)
- ✅ Grouped by ZIP code prefix (5 digits)
- ✅ Added `municipality_id` (IBGE code)
- ✅ Added `data_source` for traceability
- ✅ **Enrichment:** Added `geolocation_region`

### Data Quality
- **Before:** ~1,000,185 rows
- **After:** ~19,015 rows (unique ZIP prefixes)
- **Base dos Dados coverage:** ~85-95% match rate
- **Fallback strategy:** Uses Olist data when no match found

### Output Columns
```
geolocation_zip_code_prefix
geolocation_city (prioritizes Base dos Dados)
geolocation_state
geolocation_lat (from centroide or Olist)
geolocation_lng (from centroide or Olist)
municipality_id (IBGE code)
data_source ('basedosdados' or 'olist')
geolocation_region (created)
```

---

## 5. stg_orders

**Source:** `olist_raw.orders`  
**Primary Key:** `order_id`  
**Foreign Key:** `customer_id` → `stg_customers`

### Main Transformations
- ✅ Deduplication by `order_id`
- ✅ Standardized `order_status` (lowercase, trimmed)
- ✅ Validated timestamp sequence (purchase → approved → carrier → delivered)
- ✅ Removed orders with future purchase dates
- ✅ **FK Validation:** Only orders with valid `customer_id`
- ✅ **Flags created:**
  - `is_delivered` (BOOL)
  - `is_completed` (BOOL)
  - `is_canceled` (BOOL)

### Data Quality
- **Before:** ~99,441 rows
- **After:** ~99,441 rows (unique orders)
- **Null timestamps:** Preserved (expected for canceled/processing orders)
- **Status distribution:** ~97% delivered, ~1% canceled

### Output Columns
```
order_id
customer_id
order_status
order_purchase_timestamp
order_approved_at
order_delivered_carrier_date
order_delivered_customer_date
order_estimated_delivery_date
is_delivered (created, BOOL)
is_completed (created, BOOL)
is_canceled (created, BOOL)
```

---

## 6. stg_order_items

**Source:** `olist_raw.order_items`  
**Primary Key:** `order_id` + `order_item_id`  
**Foreign Keys:**
- `order_id` → `stg_orders`
- `product_id` → `stg_products`
- `seller_id` → `stg_sellers`

### Main Transformations
- ✅ Deduplication by `order_id` + `order_item_id`
- ✅ **FK Validation:** Inner join with `stg_orders`
- ✅ Validated monetary values (price ≥ 0, freight ≥ 0)
- ✅ Removed extreme outliers (price > R$50,000)
- ✅ **Calculated field:** `total_item_value = price + freight_value`

### Data Quality
- **Before:** ~112,650 rows
- **After:** ~112,650 rows (valid items)
- **Average price:** ~R$120
- **Average freight:** ~R$20
- **Items per order:** Most orders have 1 item

### Output Columns
```
order_id
order_item_id
product_id
seller_id
shipping_limit_date
price (validated)
freight_value (validated)
total_item_value (created)
```

---

## 7. stg_payments

**Source:** `olist_raw.payments`  
**Primary Key:** `order_id` + `payment_sequential`  
**Foreign Key:** `order_id` → `stg_orders`

### Main Transformations
- ✅ Deduplication by `order_id` + `payment_sequential`
- ✅ Standardized `payment_type` (lowercase, trimmed)
- ✅ Validated payment types (credit_card, debit_card, boleto, voucher)
- ✅ Validated `payment_value` > 0
- ✅ Validated `payment_installments` ≥ 1 (treated nulls as 1)
- ✅ **FK Validation:** Inner join with `stg_orders`
- ✅ **Flags created:**
  - `is_credit_card` (BOOL)
  - `is_boleto` (BOOL)

### Data Quality
- **Before:** ~103,886 rows
- **After:** ~103,886 rows (valid payments)
- **Payment distribution:** ~75% credit card, ~20% boleto
- **Multiple payment methods:** Some orders use >1 method

### Output Columns
```
order_id
payment_sequential
payment_type (validated)
payment_installments (validated, min 1)
payment_value (validated)
is_credit_card (created, BOOL)
is_boleto (created, BOOL)
```

---

## 8. stg_reviews

**Source:** `olist_raw.reviews`  
**Primary Key:** `review_id`  
**Foreign Key:** `order_id` → `stg_orders`

### Main Transformations
- ✅ Deduplication by `review_id` (kept most recent)
- ✅ Validated `review_score` (1-5 range)
- ✅ Cleaned text fields (trimmed whitespace)
- ✅ Preserved null comments (not all reviews have text)
- ✅ **FK Validation:** Inner join with `stg_orders`
- ✅ **Flag created:** `has_comment` (BOOL)

### Data Quality
- **Before:** ~99,224 rows
- **After:** ~99,224 rows (valid reviews)
- **Average score:** ~4.1
- **Score distribution:** ~57% gave 5 stars, ~12% gave 1 star
- **Comments:** ~40% of reviews include text comments

### Output Columns
```
review_id
order_id
review_score (validated 1-5)
review_comment_title (cleaned)
review_comment_message (cleaned)
review_creation_date
review_answer_timestamp
has_comment (created, BOOL)
```

---

## 📋 Data Quality Rules Applied

All staging tables follow these standardized rules:

### ✅ Applied Rules
1. **Data Types:**
   - IDs → STRING
   - Dates → TIMESTAMP
   - Monetary values → FLOAT64
   - Flags → BOOL

2. **Deduplication:**
   - Used `ROW_NUMBER()` with documented criteria
   - Kept first occurrence by primary key

3. **Null Handling:**
   - NOT filled arbitrarily
   - Documented with validation queries
   - Removed rows only when critical fields are null

4. **Range Validation:**
   - Positive values where applicable
   - Logical dates (no future dates, correct sequence)
   - Valid Brazilian states/coordinates

5. **Standardization:**
   - Text: trimmed, consistent case
   - Categories: controlled vocabulary
   - Preserved original columns even when creating new ones

### ❌ NOT Applied (Saved for Marts Layer)
- No aggregations
- No complex metrics (RFM, LTV, cohorts)
- No complex joins (except FK validation)
- No ML features
- No removal of original columns

---

## 🔍 Validation Queries

Each SQL file includes commented validation queries to check:
- Null percentages per column
- Row counts before/after
- Distribution of key fields
- Data quality metrics

**To run validations:**
Uncomment the validation section at the end of each SQL file and execute separately.

---

## 📈 Summary Statistics

| Table | Rows (Raw) | Rows (Staging) | Dedup Rate | Key Enrichments |
|-------|------------|----------------|------------|-----------------|
| stg_customers | 99,441 | 96,096 | 3.4% | Region |
| stg_sellers | 3,095 | 3,095 | 0% | Region |
| stg_products | 32,951 | 32,951 | 0% | English categories |
| stg_geolocation | 1,000,185 | 19,015 | 98.1% | Base dos Dados, municipality_id |
| stg_orders | 99,441 | 99,441 | 0% | Status flags |
| stg_order_items | 112,650 | 112,650 | 0% | Total value |
| stg_payments | 103,886 | 103,886 | 0% | Payment type flags |
| stg_reviews | 99,224 | 99,224 | 0% | Comment flag |

---

## 🚀 Next Steps

After completing the staging layer, proceed to:
1. **Marts Layer** - Create analytical aggregated tables
2. **Analysis** - Business intelligence queries
3. **Visualization** - Dashboard creation

---

## 📝 Notes

- All scripts use `CREATE OR REPLACE TABLE` for idempotency
- Foreign key validations use INNER JOIN to maintain referential integrity
- Base dos Dados integration provides official Brazilian geographic data
- Flags are stored as BOOL for better performance and clarity
- Original column names preserved even when containing typos (e.g., `product_name_lenght`)