# QuintoAndar Case - E-commerce Analytics

## About the Project

Comprehensive e-commerce data analysis using the Olist dataset, focusing on business metrics, customer retention, and operational performance.

**Stack:** Python | SQL | BigQuery | Looker Studio | Power BI

---

## Objectives

- Exploratory Data Analysis (EDA)
- Dimensional modeling in BigQuery
- Creation of data marts for business analysis
- Interactive dashboards in Looker Studio

---

## Project Structure Overview

```
├── notebooks/          # Exploratory analyses (9 notebooks)
│   ├── exports/       # Generated CSVs and outputs
│   └── html/          # Exported notebooks
├── sql/               
│   ├── staging/       # Cleaning layer (raw → clean)
│   └── marts/         # Analytical layer (customer, financial, operational)
├── dashboards/        # Power BI dashboards
└── data/raw/          # Original Olist dataset
```

> **Detailed analyses:** See [`notebooks/README.md`](notebooks/README.md)

---

## Some of the Data Marts Developed

| Mart | Description |
|------|-----------|
| `mart_customer_rfm` | Customer RFM segmentation |
| `mart_customer_ltv` | Customer Lifetime Value |
| `mart_cohort_retention` | Cohort retention analysis |
| `mart_revenue_summary` | Revenue and orders summary |
| `mart_unit_economics` | LTV/CAC metrics |
| `mart_delivery_performance` | Delivery and SLA performance |
| `mart_payment_analysis` | Payment methods analysis |
| `mart_geographic_performance` | Performance by state/region |

---

## Power BI Dashboards

### [Executive Overview](/dashboards/powerbi/dashboard_executive.pbix)
Key KPIs, revenue trends, and top customers

---

## Key Insights

### Delivery Performance
- **93% SLA** - good operation, but below excellence (target: ≥95%)
- **Regional bottleneck**: North/Northeast regions concentrate delays
- **Impact**: 7% delayed orders affect brand perception

### Pricing & Freight
- Products with **high freight** (>20%) have **73% lower** ticket
- Disproportionate freight limits low-value sales

### Customer Behavior
- **~96K customers**, R$15M+ total revenue
- **Low repurchase** (<10%) - growth dependent on acquisition
- **8% of customers** concentrate significantly higher LTV

### Geographic Distribution
- **Top 5 states**: ~73% of revenue (regional concentration)
- **Southeast dominates**: SP-RJ-MG as main markets
- **Opportunity**: States outside the axis with competitive LTV

---

## How to Reproduce

1. **Clone the repository**
```bash
git clone [your-repo]
cd [your-repo]
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure BigQuery**
   - Create project in GCP
   - Place credentials in `credentials/bigquery-key.json`
   - Execute queries in `sql/staging/` → `sql/marts/`

4. **Run notebooks**
```bash
jupyter notebook notebooks/
```

5. **Access dashboards**
   - Links available in the Dashboards section above

---

## Dataset

**Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

**Period:** 2016-2018  
**Records:** ~100k orders  
**Tables:** 9 CSV files (customers, orders, products, reviews, etc.)

---

## Author

**[Your Name]**  
[![LinkedIn](https://img.shields.io/badge/-LinkedIn-blue?style=flat-square&logo=Linkedin&logoColor=white)](YOUR_LINKEDIN)
[![GitHub](https://img.shields.io/badge/-GitHub-black?style=flat-square&logo=github)](YOUR_GITHUB)

---

## License

This project is under the MIT license.