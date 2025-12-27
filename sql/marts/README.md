# Marts Layer - Olist E-commerce Analysis

## Overview
The marts layer contains aggregated, business-ready tables optimized for analytics and visualization. These tables combine data from multiple staging tables to answer specific business questions.

---

## 📊 Marts Architecture

### Execution Dependencies

```
STAGING LAYER (already created)
    ↓
CUSTOMER MARTS (execute in order)
    ↓
OPERATIONAL MARTS (can run in parallel)
    ↓
FINANCIAL MARTS (depend on customer marts)
```

---

## 🎯 Customer Marts (5 tables)

### 1. mart_customer_base
**Purpose:** Consolidated customer view with all key metrics  
**Sources:** `stg_customers`, `stg_orders`, `stg_payments`, `stg_reviews`  
**Key Metrics:**
- Total orders per customer
- Total revenue per customer
- Average review score
- First and last purchase dates
- Customer lifetime (days)
- Customer status (active/churned)

**Business Use:** Customer 360° view, segmentation foundation

---

### 2. mart_customer_ltv
**Purpose:** Calculate Lifetime Value by customer and geography  
**Sources:** `mart_customer_base`  
**Key Metrics:**
- LTV per customer
- Average Order Value (AOV)
- Customer lifespan (days/months)
- Orders per month
- LTV by state/region
- Revenue concentration

**Business Use:** Geographic expansion decisions, customer value optimization

**Related Analysis:** Ranking #1 - "LTV by State / Geographic Performance"

---

### 3. mart_customer_rfm
**Purpose:** RFM (Recency, Frequency, Monetary) segmentation  
**Sources:** `mart_customer_base`  
**Key Metrics:**
- Recency score (1-5)
- Frequency score (1-5)
- Monetary score (1-5)
- RFM segment label (Champions, Loyal, At Risk, Lost, etc.)
- Segment distribution
- Segment characteristics

**RFM Segments:**
- Champions (555, 554, 544, 545)
- Loyal Customers (543, 444, 435)
- Potential Loyalists (553, 551, 552)
- New Customers (512, 511, 422)
- At Risk (255, 254, 245)
- Can't Lose Them (155, 154, 144)
- Lost (111, 112, 121)
- [+ 7 more segments]

**Business Use:** Marketing campaign targeting, retention strategies

**Related Analysis:** Ranking #4 - "RFM Analysis"

---

### 4. mart_customer_cohort_retention
**Purpose:** Track customer retention by cohort (first purchase month)  
**Sources:** `stg_orders`, `stg_payments`  
**Key Metrics:**
- Cohort month (first purchase)
- Cohort size (unique customers)
- Retention rate by month (0, 1, 3, 6, 12)
- Revenue retention
- Churn rate by cohort
- Average retention curve

**Business Use:** Product-market fit validation, retention optimization

**Related Analysis:** Ranking #2 - "Cohort Retention Analysis"

---

### 5. mart_customer_segments
**Purpose:** Enriched customer segmentation combining RFM + LTV + behavior  
**Sources:** `mart_customer_rfm`, `mart_customer_ltv`, `mart_customer_base`  
**Key Metrics:**
- Combined segment (RFM + LTV tier)
- Segment characteristics (avg metrics)
- Segment size and revenue share
- Recommended actions per segment
- Migration patterns between segments

**Business Use:** Strategic planning, personalized customer experience

---

## 🔧 Operational Marts (4 tables)

### 6. mart_category_performance
**Purpose:** Product category analysis  
**Sources:** `stg_order_items`, `stg_products`, `stg_orders`  
**Key Metrics:**
- Revenue by category
- Orders by category
- Units sold
- Average price
- Category growth (MoM, YoY)
- Category share
- Repeat purchase rate per category

**Business Use:** Inventory planning, category expansion decisions

**Related Analysis:** Ranking #12 - "Category Performance"

---

### 7. mart_delivery_performance
**Purpose:** Logistics and delivery metrics  
**Sources:** `stg_orders`, `stg_customers`, `stg_sellers`, `stg_order_items`, `stg_reviews`  
**Key Metrics:**
- Actual delivery time
- Estimated delivery time
- Delivery delay (days)
- SLA compliance rate
- On-time delivery %
- Delay by route (origin state → destination state)
- Correlation with review scores
- Average freight cost

**Business Use:** Logistics optimization, SLA improvement, seller performance

**Related Analysis:** Ranking #6 - "Delivery Analysis"

---

### 8. mart_payment_analysis
**Purpose:** Payment method analysis and conversion  
**Sources:** `stg_payments`, `stg_orders`  
**Key Metrics:**
- Volume by payment type
- Revenue by payment type
- AOV by payment method
- Conversion rate by method
- Cancellation rate by method
- Average installments
- Payment distribution (1x, 2-3x, 4-6x, etc.)
- Multiple payment methods %

**Business Use:** Payment optimization, fraud prevention, conversion improvement

**Related Analysis:** Ranking #5 - "Payment Analysis"

---

### 9. mart_seller_performance
**Purpose:** Marketplace seller analytics  
**Sources:** `stg_sellers`, `stg_order_items`, `stg_reviews`, `stg_orders`  
**Key Metrics:**
- Revenue by seller
- Orders by seller
- Average review score
- Delivery performance
- Product diversity
- Active months
- Seller concentration (Pareto)

**Business Use:** Seller management, marketplace health monitoring

**Related Analysis:** Ranking #16 - "Seller Performance"

---

## 💰 Financial Marts (3 tables)

### 10. mart_revenue_summary
**Purpose:** Time-series revenue aggregation  
**Sources:** `stg_orders`, `stg_payments`, `mart_customer_base`  
**Key Metrics:**
- Revenue by day/week/month
- New vs repeat customer revenue
- Growth rates (MoM, YoY)
- Cumulative revenue
- Revenue by segment
- Seasonality patterns

**Business Use:** Financial reporting, forecasting, board presentations

---

### 11. mart_unit_economics
**Purpose:** Economics by customer segment and geography  
**Sources:** `mart_customer_ltv`, `mart_customer_rfm`, `stg_order_items`  
**Key Metrics:**
- LTV by segment
- CAC proxy (estimated)
- LTV/CAC ratio
- Payback period
- Gross margin estimate
- Contribution margin
- Unit economics by state

**Business Use:** Investment decisions, profitability optimization

**Related Analysis:** Ranking #8 - "Unit Economics Analysis"

---

### 12. mart_geographic_performance
**Purpose:** State-level performance analysis  
**Sources:** `stg_customers`, `mart_customer_ltv`, `stg_orders`, `stg_geolocation`  
**Key Metrics:**
- Revenue by state/region
- Customer count by state
- LTV by state
- AOV by state
- Orders per customer by state
- Market penetration
- Growth potential by state

**Business Use:** Geographic expansion strategy, regional marketing

**Related Analysis:** Ranking #1 - "LTV by State / Geographic Performance"

---

## 🔄 Execution Order

### Phase 1: Customer Marts (SEQUENTIAL)
Execute in this order due to dependencies:

1. ✅ `mart_customer_base.sql` **(FOUNDATION - run first)**
2. ✅ `mart_customer_ltv.sql` (depends on #1)
3. ✅ `mart_customer_rfm.sql` (depends on #1)
4. ✅ `mart_customer_cohort_retention.sql` (depends on staging only)
5. ✅ `mart_customer_segments.sql` (depends on #2, #3)

### Phase 2: Operational Marts (PARALLEL)
These can run simultaneously:

6. ⚡ `mart_category_performance.sql`
7. ⚡ `mart_delivery_performance.sql`
8. ⚡ `mart_payment_analysis.sql`
9. ⚡ `mart_seller_performance.sql`

### Phase 3: Financial Marts (SEQUENTIAL)
Execute after Phase 1 is complete:

10. ✅ `mart_revenue_summary.sql`
11. ✅ `mart_unit_economics.sql` (depends on mart_customer_ltv)
12. ✅ `mart_geographic_performance.sql` (depends on mart_customer_ltv)

---

## 📈 Expected Table Sizes

| Mart | Granularity | Est. Rows | Update Frequency |
|------|-------------|-----------|------------------|
| mart_customer_base | 1 row per customer | ~96,000 | Daily |
| mart_customer_ltv | 1 row per customer | ~96,000 | Daily |
| mart_customer_rfm | 1 row per customer | ~96,000 | Daily |
| mart_customer_cohort_retention | 1 row per cohort-month | ~1,000 | Daily |
| mart_customer_segments | 1 row per segment | ~15 | Daily |
| mart_category_performance | 1 row per category | ~70 | Daily |
| mart_delivery_performance | 1 row per route | ~500 | Daily |
| mart_payment_analysis | 1 row per payment type | ~5 | Daily |
| mart_seller_performance | 1 row per seller | ~3,000 | Daily |
| mart_revenue_summary | 1 row per day | ~800 | Daily |
| mart_unit_economics | 1 row per segment | ~20 | Weekly |
| mart_geographic_performance | 1 row per state | ~27 | Daily |

---

## 🎯 Key Business Questions Answered

### Customer Understanding
- Who are our most valuable customers? → `mart_customer_ltv`
- Which customers are at risk of churning? → `mart_customer_rfm`
- How well do we retain customers? → `mart_customer_cohort_retention`
- What customer segments exist? → `mart_customer_segments`

### Operational Excellence
- Which categories drive the most revenue? → `mart_category_performance`
- Are we meeting delivery expectations? → `mart_delivery_performance`
- Which payment methods convert best? → `mart_payment_analysis`
- Which sellers are top performers? → `mart_seller_performance`

### Financial Performance
- How is revenue trending? → `mart_revenue_summary`
- Are we profitable by segment? → `mart_unit_economics`
- Where should we expand? → `mart_geographic_performance`

---

## 🚀 Next Steps

After creating all marts:
1. **Validation:** Run data quality checks on all marts
2. **Documentation:** Document key findings from each mart
3. **Visualization:** Connect to BI tool (Looker Studio, Tableau, etc.)
4. **Analysis:** Create deep-dive notebooks for priority insights
5. **Presentation:** Build executive dashboard

---

## 📝 Notes

- All marts use `CREATE OR REPLACE TABLE` for idempotency
- Marts are denormalized for query performance
- Date calculations use reference date of analysis execution
- NULL handling is explicit and documented in each mart
- All monetary values are in BRL (Brazilian Real)