# Notebooks – Detailed Analysis

This document presents a comprehensive view of the analytical pipeline, connecting each analysis stage and its main results.

---

# 01 — Exploratory Data Analysis (EDA)

## Objective
The objective of this notebook is to **develop a comprehensive understanding of the business and the underlying dataset**, providing an initial overview of customers, revenue, purchasing behavior, and geographic distribution.

This EDA serves as the **analytical foundation** for all subsequent analyses, aiming to uncover patterns, trends, anomalies, and high-level opportunities that justify deeper investigation in later stages of the project.

---

## Methodology
The methodology follows a structured, business-oriented exploratory data analysis approach:

- **Data validation and quality assessment**
  - Identification of missing values, duplicates, and outliers, with particular attention to revenue and LTV-related variables.

- **Descriptive analysis**
  - Aggregated metrics for customers, orders, revenue, and average order value.

- **Distribution analysis**
  - Examination of LTV, purchase counts, ratings, product categories, and revenue concentration.

- **Temporal analysis**
  - Monthly revenue evolution and identification of seasonality patterns.

- **Geographic analysis**
  - Distribution of customers and revenue by state and region.

- **Initial behavioral segmentation**
  - Identification of single-purchase versus repeat customers.

No statistical or predictive modeling is applied at this stage. The focus is **exploratory and diagnostic**, preparing the ground for more advanced analyses.

---

## Main Results
The main quantitative and structural results observed include:

- **Business scale**
  - Approximately 96,000 customers generating over R$15 million in total revenue.

- **Revenue dynamics**
  - Revenue growth over time with clear monthly seasonality.
  - A business model strongly driven by customer acquisition, with new customers accounting for most revenue.

- **Customer behavior**
  - Strong predominance of one-time buyers.
  - Highly right-skewed LTV distribution, where a small fraction of customers concentrates a large share of total value.

- **Perceived quality**
  - Average customer ratings above 4, indicating generally positive satisfaction.

- **Geographic concentration**
  - Strong revenue concentration in the Southeast region (São Paulo, Rio de Janeiro, Minas Gerais).
  - The top five states account for more than 70% of total revenue.

- **Product categories**
  - Revenue concentrated in a limited number of categories, with a clear long-tail pattern.

---

## Key Insights
Several strategic insights emerge from the analysis:

- **Retention is the primary bottleneck**
  - Low repurchase rates indicate that current growth is fragile without retention initiatives.

- **Customer value is highly concentrated**
  - A small subset of customers generates a disproportionate share of revenue, making segmentation critical.

- **Strong dependence on acquisition**
  - High inflow of new customers is not matched by conversion into repeat buyers.

- **Clear geographic opportunity**
  - Regions outside the Southeast present potential for expansion and diversification.

- **Good experience does not imply loyalty**
  - Positive ratings coexist with low recurrence, suggesting gaps after the first purchase (onboarding, incentives, relationship management).

---

## Connection to Next Analyses
This notebook **explicitly defines the analytical roadmap** followed in subsequent notebooks:

- **RFM Analysis (Notebook 02)**
  - Translates value concentration into actionable customer segmentation.

- **Cohort & Retention Analysis (Notebook 03)**
  - Quantifies and explains the retention problem identified in the EDA.

- **Geographic LTV Analysis (Notebook 04)**
  - Explores regional disparities in customer value.

- **Churn Prediction (Notebook 05)**
  - Enables proactive identification of customers at risk of churn.

The EDA establishes the baseline understanding required to move from descriptive insights to predictive and strategic analytics.

---

# 02 — RFM Analysis

## Objective
The objective of this notebook is to apply the **RFM framework (Recency, Frequency, Monetary)** to segment the customer base according to purchasing behavior and generated value. This analysis transforms the high-level patterns identified in the EDA into **actionable customer groups** that can directly support business decision-making.

The analysis aims to answer:
- Who are the most valuable customers?
- Which customers are at risk of churn?
- Where should retention and relationship efforts be concentrated?

---

## Methodology
The methodology follows the classic **RFM framework**, adapted to the business context:

- **Definition of RFM metrics**
  - **Recency**: time elapsed since the customer’s last purchase.
  - **Frequency**: total number of purchases made by the customer.
  - **Monetary**: total revenue generated by the customer.

- **Scoring and normalization**
  - Each metric is transformed into relative scores, typically using quantile-based ranking.
  - Individual R, F, and M scores are combined into a consolidated RFM score.

- **Segment classification**
  - Customers are grouped into interpretable segments such as:
    - Champions
    - Loyal Customers
    - At Risk
    - Lost / Low Value
  - The segmentation prioritizes **business interpretability** rather than purely statistical clustering.

- **Descriptive analysis of segments**
  - Distribution of customers, revenue, and average order value by segment.
  - Comparison between segment size and financial contribution.

---

## Main Results
The main results observed include:

- **Strong revenue concentration**
  - A small number of customers, belonging to high-RFM segments, account for a significant share of total revenue.

- **Predominance of low-engagement customers**
  - Most customers exhibit low frequency and low recency, confirming patterns identified in the EDA.

- **Clearly defined risk segments**
  - Customers with historically high value but low recency form a critical group for retention actions.

- **Imbalance between volume and value**
  - Segments with many customers are not necessarily the most financially relevant.

---

## Key Insights
Several strategic insights emerge from the RFM segmentation:

- **Not all customers deserve the same level of investment**
  - Uniform strategies waste resources; segmentation highlights where potential ROI is highest.

- **Retention should be prioritized over acquisition**
  - “At Risk” customers already demonstrated value and have lower marginal recovery cost than new customers.

- **Champions are few but strategically critical**
  - These customers justify personalized actions, exclusive benefits, and dedicated communication.

- **Inactive customers are not homogeneous**
  - There is a clear distinction between low-value customers and previously valuable customers who disengaged.

---

## Connection to Next Analyses
This notebook acts as a **bridge between exploratory analysis and advanced analytics**, connecting directly to subsequent steps in the pipeline:

- **Cohort & Retention Analysis**
  - To understand how RFM segments evolve over time.

- **LTV Analysis**
  - To quantify expected future value by segment.

- **Churn Prediction**
  - RFM variables and segments serve as strong baseline features for predictive models.

- **Advanced Customer Segmentation**
  - Enables comparison between rule-based segmentation (RFM) and machine learning–driven clustering.

The RFM analysis consolidates the transition from **“what happened”** to **“who should be prioritized.”**


---

# 03 — Cohort & Retention Analysis

## Objective
The objective of this notebook is to **analyze customer retention over time** using **cohort analysis**, with the goal of understanding how different groups of customers behave after their first purchase.

This analysis seeks to answer:
- At what point do customers disengage from the business?
- Does retention improve or deteriorate over time?
- Are there structural patterns of early churn across cohorts?

This notebook deepens and quantifies the main issue identified in the EDA and RFM analyses: **low repeat purchase rates and high early churn**.

---

## Methodology
The methodology follows the classical **Cohort Analysis** framework, structured as follows:

- **Cohort definition**
  - Customers are grouped by the **month of their first purchase** (`cohort_month`).

- **Time alignment**
  - Creation of a `months_since_first_purchase` variable to track customer activity over time relative to their cohort start.

- **Retention calculation**
  - For each cohort, the proportion of active customers is calculated for each subsequent month.
  - Retention is analyzed using both tabular outputs and visualizations (e.g., retention heatmaps).

- **Aggregated analysis**
  - Calculation of average retention metrics (e.g., Month 1, Month 2, Month 3 retention).
  - Identification of recurring churn patterns across cohorts.

The methodology is fully descriptive and analytical, with no predictive modeling applied at this stage.

---

## Main Results
The key findings from the cohort and retention analysis include:

- **Extremely high early churn**
  - The largest customer drop occurs immediately after the first month (Month 1).

- **Declining and unstable retention**
  - After the initial months, the remaining customer base stabilizes at consistently low retention levels.

- **Minimal improvement across cohorts**
  - More recent cohorts do not show meaningful retention improvements compared to older cohorts.

- **Quantitative confirmation of low recurrence**
  - The results objectively validate the low repeat purchase patterns previously observed in EDA and RFM analyses.

---

## Key Insights
Several critical insights emerge from the retention patterns:

- **The churn problem is immediate, not delayed**
  - The highest-impact intervention window is within the first 30–60 days after the first purchase.

- **Acquisition without retention leads to fragile growth**
  - The business grows in volume but fails to build a stable recurring customer base.

- **Early customer experience is decisive**
  - Onboarding, delivery quality, communication, and incentives for a second purchase are key leverage points.

- **Lack of structural improvement over time**
  - The absence of better retention in newer cohorts suggests limited learning or systemic changes in customer engagement strategy.

---

## Connection to Next Analyses
This notebook serves as a **central link in the analytical pipeline**, connecting descriptive insights to strategic and predictive analyses:

- **Churn Prediction**
  - The temporal churn dynamics inform target definition and feature engineering for predictive models.

- **LTV Analysis**
  - Observed retention patterns directly explain the limited lifetime value identified in subsequent analyses.

- **Customer Segmentation**
  - Segments can be prioritized based on distinct retention behaviors over time.

- **Payment, Delivery, and Financial Analyses**
  - These analyses investigate whether operational or financial frictions contribute to early customer loss.

The cohort analysis transforms the perception of “low retention” into **clear temporal evidence**, guiding short-term actions with the highest potential impact.


---


# 04 — LTV & Geographic Analysis

## Objective
This notebook aims to analyze **Customer Lifetime Value (LTV) across geographic dimensions**, identifying how customer value varies by state and region. The goal is to move beyond aggregate LTV metrics and uncover **where the most valuable customers are located**, as well as regions that represent growth opportunities or strategic risks.

This analysis supports geographically informed decisions related to marketing investment, expansion strategy, and operational prioritization.

---

## Methodology
The analysis follows a descriptive and comparative approach:

1. **LTV Calculation**
   - LTV is computed at the customer level based on historical revenue.
   - Aggregations are performed to obtain average LTV by state and region.

2. **Geographic Aggregation**
   - Customers and revenue are grouped by state and macro-region.
   - Metrics include total customers, total revenue, average LTV, and revenue concentration.

3. **Comparative Analysis**
   - States are classified into strategic groups such as:
     - High volume / high LTV
     - High volume / low LTV
     - Low volume / high LTV
     - Early-stage markets
   - Outliers and extreme values are identified to avoid misleading conclusions.

4. **Strategic Interpretation**
   - Results are translated into business-oriented categories (e.g., quick wins, growth opportunities, key risks).

---

## Key Results
The main findings from the analysis include:

- **Strong geographic concentration of value**
  - A small number of states account for a disproportionate share of customers and revenue.
- **High-LTV states are not always the largest**
  - Some states show above-average LTV despite lower customer volume.
- **Clear performance gaps between regions**
  - Significant differences in customer value exist across regions, indicating uneven market maturity.
- **Identifiable “quick win” markets**
  - Certain states combine a solid customer base with above-average LTV, representing immediate optimization opportunities.

---

## Key Insights
- **Geography materially impacts customer value**
  - LTV is not homogeneous across the country; location is a meaningful segmentation dimension.
- **Expansion should be selective, not uniform**
  - Investing equally across all regions risks over-allocating resources to low-potential markets.
- **High-LTV regions deserve differentiated strategies**
  - Premium markets justify higher acquisition costs, better service levels, and targeted retention actions.
- **Early-stage regions require caution**
  - Low LTV and low volume regions should be treated as experimental or long-term bets, not short-term growth drivers.

---

## Connection to Next Analyses
This notebook connects directly to subsequent stages of the analytical pipeline:

- **Delivery Analysis**
  - Investigates whether logistics performance explains regional differences in LTV.
- **Payment Analysis**
  - Explores if payment behavior varies geographically and impacts customer value.
- **Financial Analysis**
  - Uses geographic LTV to assess unit economics and profitability by region.
- **Churn Prediction**
  - Geographic features can enhance churn models by capturing structural regional effects.
- **Customer Segmentation**
  - Combines geographic and behavioral dimensions to create more actionable segments.

This analysis bridges **customer value and spatial strategy**, enabling data-driven geographic prioritization.


---

# 05 — Churn Prediction

## Objective
This notebook aims to **predict customer churn** by building a supervised machine learning model capable of identifying customers who are likely to stop purchasing. The goal is to move from descriptive analyses (EDA, RFM, Cohort) to a **forward-looking, action-oriented approach**, enabling proactive retention strategies.

The analysis seeks to answer:
- Which customers are at higher risk of churn?
- Which behavioral and transactional features are most predictive of churn?
- How can churn risk be operationalized for business actions?

---

## Methodology
The notebook follows a standard churn modeling pipeline:

1. **Churn Definition**
   - Customers are labeled as churned based on inactivity over a predefined time window.
   - The target variable is binary (churn vs. active).

2. **Feature Engineering**
   - Behavioral features derived from transaction history (recency, frequency, monetary value).
   - Aggregated customer metrics such as order count, total spend, average ticket, and tenure.
   - Features previously explored in EDA, RFM, and Cohort analyses are reused in structured form.

3. **Data Preparation**
   - Handling of missing values.
   - Feature scaling where applicable.
   - Train-test split to evaluate generalization performance.

4. **Model Training**
   - Supervised classification model trained to predict churn probability.
   - Baseline performance is evaluated using appropriate classification metrics.

5. **Model Evaluation**
   - Assessment using metrics such as accuracy, precision, recall, and ROC-AUC.
   - Feature importance analysis to interpret model behavior.

---

## Key Results
The main outcomes of the analysis include:

- **Feasible churn predictability**
  - Customer churn can be predicted with performance significantly better than random baseline.
- **Behavioral features dominate**
  - Recency and frequency-related variables are the strongest predictors of churn.
- **Clear separation of risk profiles**
  - The model distinguishes low-risk loyal customers from high-risk disengaged ones.
- **Actionable probability output**
  - The model produces churn probabilities rather than binary labels, enabling prioritization.

---

## Key Insights
- **Churn is largely driven by disengagement**
  - Time since last purchase is the single most important signal.
- **High-value customers can still churn**
  - Monetary value alone does not guarantee retention; behavior matters more than spend.
- **Early intervention is critical**
  - Customers show clear warning signs before fully churning.
- **Model interpretability supports trust**
  - Feature importance aligns with patterns observed in earlier analyses, reinforcing analytical consistency.

---

## Connection to Next Analyses
This notebook is a critical transition point from analysis to activation and connects directly to subsequent work:

- **Customer Segmentation**
  - Churn risk can be combined with RFM or clustering to create risk-aware segments.
- **LTV Analysis**
  - Enables calculation of risk-adjusted LTV (expected future value).
- **Payment Analysis**
  - Investigates whether payment behavior correlates with churn risk.
- **Delivery Analysis**
  - Explores whether operational issues contribute to predicted churn.
- **Financial Analysis**
  - Supports estimating the financial impact of churn and prioritizing retention investments.

This notebook operationalizes prior insights, transforming historical patterns into **predictive intelligence** for decision-making.

---

# 06 — Delivery Analysis

## Objective
This notebook aims to analyze **delivery and logistics performance** to understand how operational factors affect customer experience and business outcomes. The focus is on identifying inefficiencies, regional disparities, and delivery issues that may contribute to dissatisfaction, churn, or reduced customer value.

The analysis seeks to answer:
- How reliable is the delivery operation overall?
- Are there significant differences in delivery performance across regions?
- Do delivery issues represent a structural risk to retention and LTV?

---

## Methodology
The analysis follows a descriptive, operations-focused approach:

1. **Definition of Delivery Metrics**
   - Delivery time, freight cost, SLA compliance, and delivery issue indicators.
   - Creation of binary flags for delivery problems and SLA violations.

2. **Data Aggregation**
   - Aggregation of delivery metrics at order, customer, and regional levels.
   - Calculation of overall and regional SLA compliance rates.

3. **Comparative Analysis**
   - Comparison of delivery performance across regions and freight cost ranges.
   - Identification of best- and worst-performing regions.

4. **Insight Generation**
   - Translation of operational metrics into business-relevant insights and priorities.
   - Identification of high-impact operational improvement opportunities.

The methodology is diagnostic rather than predictive, aiming to explain observed outcomes.

---

## Key Results
The main results of the analysis include:

- **High overall SLA compliance with regional variance**
  - While average SLA compliance is strong, performance varies significantly by region.
- **Consistent underperformance in specific regions**
  - Certain regions systematically fall below the overall SLA average.
- **Relationship between freight cost and performance**
  - Mid-to-low freight cost ranges tend to show better SLA compliance.
- **Concentration of delivery issues**
  - A relatively small share of orders accounts for most delivery problems.

---

## Key Insights
- **Operational performance is uneven**
  - Aggregate metrics hide meaningful regional and operational disparities.
- **Delivery issues are not random**
  - Problems cluster around specific regions and logistics conditions.
- **Logistics directly impact customer experience**
  - Delivery reliability is a critical component of perceived service quality.
- **Targeted improvements offer high ROI**
  - Focusing on worst-performing regions can generate disproportionate gains.

---

## Connection to Next Analyses
This notebook connects operational performance to broader business analyses:

- **Churn Prediction**
  - Delivery issues and SLA violations can be incorporated as churn risk drivers.
- **Customer Segmentation**
  - Identifies segments disproportionately affected by delivery problems.
- **LTV & Financial Analysis**
  - Evaluates whether delivery inefficiencies erode customer lifetime value or margins.
- **Payment Analysis**
  - Enables investigation of combined operational and transactional friction.
- **Operational Optimization**
  - Serves as a baseline for testing logistics improvements and monitoring impact.

This analysis links **operational execution to customer retention and value**, reinforcing the importance of logistics as a strategic lever.


---

# 07 — Payment Analysis

## Objective
This notebook aims to analyze **customer payment behavior and payment-related events** to understand how transactional friction impacts customer experience, retention, and business performance. The focus is on identifying patterns in payment methods, failures, installments, and approval dynamics that may contribute to churn or reduced customer value.

The analysis seeks to answer:
- How do customers behave in terms of payment methods and conditions?
- Are payment issues frequent and concentrated in specific segments?
- Does payment friction represent a structural risk to retention and revenue?

---

## Methodology
The analysis follows a descriptive and diagnostic approach:

1. **Payment Data Exploration**
   - Inspection of payment methods, number of installments, payment values, and approval status.
   - Validation of data consistency and distribution of payment-related variables.

2. **Aggregation and Segmentation**
   - Aggregation of payment metrics at customer and order levels.
   - Comparison of behavior across payment methods and installment ranges.

3. **Failure and Friction Analysis**
   - Identification of failed, delayed, or problematic payments.
   - Measurement of concentration of payment issues among customers and transactions.

4. **Business Interpretation**
   - Translation of transactional patterns into potential business risks and opportunities.
   - Identification of payment-related pain points that may affect customer lifetime value.

No predictive models are trained in this notebook; the focus is on understanding transactional behavior.

---

## Key Results
The main results observed include:

- **Strong concentration in a limited set of payment methods**
  - A small number of payment methods dominate transaction volume and revenue.
- **Installment usage is widespread**
  - A significant share of customers relies on installment payments rather than single payments.
- **Payment issues are relatively infrequent but non-negligible**
  - Failed or problematic payments represent a small percentage of total transactions but are not evenly distributed.
- **Higher friction in specific payment configurations**
  - Certain combinations of payment method and installment count show higher rates of issues.

---

## Key Insights
- **Payment friction is a hidden churn driver**
  - Even low-frequency payment failures can disproportionately affect customer trust and repeat purchase behavior.
- **Installment-heavy behavior signals price sensitivity**
  - Customers using many installments may be more vulnerable to churn or financial stress.
- **Not all revenue is equally reliable**
  - Nominal revenue figures may mask underlying payment risk.
- **Payment experience is part of the customer journey**
  - Smooth transactions are as critical as delivery and product quality.

---

## Connection to Next Analyses
This notebook connects payment behavior to broader analytical themes:

- **Churn Prediction**
  - Payment failures, installment patterns, and approval issues can serve as predictive features.
- **Customer Segmentation**
  - Enables separation of customers by financial behavior and payment reliability.
- **LTV & Financial Analysis**
  - Supports risk-adjusted revenue and lifetime value calculations.
- **Delivery Analysis**
  - Allows combined analysis of operational and transactional friction.
- **Financial Performance**
  - Provides input for assessing cash flow stability and default risk.

This analysis positions payment behavior as a **core component of customer experience and financial sustainability**, rather than a purely operational concern.

---

# 08 — Financial Analysis

## Objective
This notebook aims to analyze the **financial performance of the business**, focusing on revenue composition, costs, margins, and profitability. The objective is to move beyond customer-level metrics and evaluate whether growth, retention, and operational patterns observed in previous analyses translate into **sustainable financial outcomes**.

The analysis seeks to answer:
- How healthy is the business from a financial perspective?
- Which components drive revenue and which erode margins?
- Are there structural financial risks hidden behind topline growth?

---

## Methodology
The analysis follows a descriptive and diagnostic financial approach:

1. **Revenue Analysis**
   - Aggregation of total revenue over time.
   - Breakdown of revenue by relevant dimensions (orders, customers, regions where applicable).

2. **Cost and Margin Evaluation**
   - Analysis of cost components associated with transactions.
   - Calculation of gross margin and margin distribution across orders or customers.

3. **Profitability Distribution**
   - Identification of profitable vs. low-margin or loss-generating transactions.
   - Evaluation of how concentrated profits are across customers or segments.

4. **Temporal Analysis**
   - Assessment of financial trends over time to identify stability, growth, or volatility.
   - Detection of periods with margin compression or improvement.

The methodology is analytical and exploratory, without forecasting or optimization models.

---

## Key Results
The main results of the analysis include:

- **Revenue concentration**
  - A relatively small subset of customers or orders contributes disproportionately to total revenue.
- **Margin variability**
  - Significant dispersion in margins across transactions, indicating uneven profitability.
- **Cost sensitivity**
  - Certain operational or transactional patterns are associated with lower margins.
- **Topline growth does not guarantee profitability**
  - High revenue periods are not always aligned with strong margins.

---

## Key Insights
- **Not all growth is healthy**
  - Revenue expansion can mask underlying margin erosion if costs are not controlled.
- **Profitability is highly concentrated**
  - The business relies on a limited portion of customers or transactions for financial sustainability.
- **Operational and financial dimensions are tightly linked**
  - Delivery and payment inefficiencies observed earlier likely contribute to margin pressure.
- **Financial visibility is essential for prioritization**
  - Customer and operational strategies should be evaluated through a profitability lens, not revenue alone.

---

## Connection to Next Analyses
This notebook connects financial performance to the broader analytical framework:

- **Customer Segmentation**
  - Enables identification of high-revenue but low-profit versus truly high-value customers.
- **LTV Analysis**
  - Supports refinement of LTV using margin-aware or profit-based definitions.
- **Churn Prediction**
  - Allows prioritization of retention efforts based on financial impact.
- **Payment and Delivery Analysis**
  - Provides a financial benchmark to quantify the cost of operational and transactional friction.
- **Strategic Decision-Making**
  - Informs pricing, discounting, and investment decisions grounded in unit economics.

This analysis grounds customer and operational insights in **financial reality**, ensuring that strategic actions are aligned with long-term profitability.


---

# 09 - Customer Segmentation Analysis

## Objective
The objective of this analysis is to segment customers into distinct groups based on their behavioral and transactional characteristics. By identifying homogeneous customer segments, the analysis aims to support more targeted marketing strategies, personalized communication, and improved decision-making related to customer value management.

## Methodology
The notebook applies an unsupervised learning approach to customer segmentation, following these main steps:
- Selection and preparation of relevant customer-level features derived from transactional data.
- Feature scaling and normalization to ensure comparability across variables.
- Application of clustering algorithms (primarily K-Means) to group customers based on similarity.
- Evaluation of the optimal number of clusters using methods such as the Elbow Method and cluster interpretability.
- Profiling of each cluster by analyzing aggregated metrics and behavioral patterns.

## Main Results
- Customers were successfully grouped into a limited number of distinct segments with clearly differentiated behaviors.
- Each cluster exhibits unique patterns in terms of purchase frequency, monetary value, and engagement.
- The segmentation highlights both high-value customers and lower-engagement or at-risk groups.
- Cluster centroids reveal meaningful contrasts between frequent, high-spending customers and occasional, low-value ones.

## Key Insights
- A small subset of customers typically contributes disproportionately to total revenue, reinforcing the importance of retention strategies for high-value segments.
- Some segments show low frequency but relatively high average order value, suggesting opportunities for reactivation or upselling.
- Other clusters represent price-sensitive or low-engagement customers, where cost-efficient communication strategies may be more appropriate.
- Behavioral segmentation provides more actionable insights than demographic-only grouping.

## Connection to Next Analyses
- The identified segments can be integrated into churn prediction models to improve predictive performance.
- Segmentation results can be combined with lifetime value (LTV) analysis to prioritize investments by customer group.
- Future analyses may explore segment evolution over time to detect migration patterns between clusters.
- The clusters can serve as input features for personalization, recommendation systems, and targeted marketing experiments.



---

