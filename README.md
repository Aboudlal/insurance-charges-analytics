# 📊 Insurance Charges Analytics -- Full BI Project

### *Storytelling + Data Warehouse + OLAP + Power BI*


![cover](https://github.com/Aboudlal/insurance-charges-analytics/blob/main/docs/images/cover.png)

------------------------------------------------------------------------

# **1. Business Goal**

The goal of this BI project is to understand **which patient groups
generate the highest medical insurance charges**, and how this
information can support fair and data‑driven pricing strategies,
preventive health programs, and ethical insurance decision‑making.

------------------------------------------------------------------------

# **2. Data Source**

**Dataset:** Medical Insurance Charges\
**Source:** https://www.kaggle.com/datasets/mirichoi0218/insurance

### **Files Used**

-   `data/raw/insurance.csv` → original\
-   `data/prepared/insurance_prepared.csv` → cleaned\
-   `data/olap_cubing_outputs/multidimensional_olap_cube.csv` → OLAP
    results\
-   `data/dw/insurance_dw.db` → SQLite Data Warehouse

------------------------------------------------------------------------

# **3. Tools Used**

-   Python (Pandas, ETL, OLAP)
-   SQLite (Star Schema DW)
-   Power BI Desktop
-   uv environment
-   Git + GitHub

------------------------------------------------------------------------

# **4. Workflow & Logic**

## **4.1 Data Preparation**

📄 Script: `prepare_insurance_data.py`

Steps: - Clean column names\
- Convert types\
- Create new groups:\
- Age Group\
- BMI Category\
- Smoker Flag\
- Remove duplicates\
- Export cleaned CSV

Output:

    data/prepared/insurance_prepared.csv

------------------------------------------------------------------------

## **4.2 Data Warehouse (SQLite)**

📄 Script: `etl_to_dw_insurance.py`

### **Star Schema**

#### Dimensions

-   **dim_region**
-   **dim_risk** (smoker & bmi_category)
-   **dim_age**

#### Fact Table

-   **fact_insurance_charges**\
    Includes: age, bmi, children, charges, keys...

Database location:

    data/dw/insurance_dw.db

------------------------------------------------------------------------

## **4.3 OLAP Cubing**

📄 Script: `olap_insurance_cubing.py`

Dimensions:

    Age Group × Smoker × Region × BMI Category

Metrics:

    charges: sum, avg, count

Output:

    data/olap_cubing_outputs/multidimensional_olap_cube.csv

------------------------------------------------------------------------

## **4.4 Power BI Analysis & Visuals**

Visualizations created: - Avg Charges by Smoker\
- Charges by BMI Category\
- Region × BMI Category matrix\
- Age Group × Smoker matrix\
- Slicers: region, smoker, age group, bmi

OLAP operations performed: - **Slicing**: filter on smoker\
- **Dicing**: multiple filters (e.g., obese + smoker + southeast)\
- **Drilldown**: Region → BMI → Age → Smoker

------------------------------------------------------------------------

# **5. Storytelling & Results**

### 🔹 **The Story**

Insurance costs vary massively across individuals.\
Using BI techniques, we uncover the *why*:

### **1. Smokers pay 3--4× more**

This is the strongest predictor of high charges.

### **2. Age multiplies risk**

Older smokers = extremely high charges.

### **3. BMI creates sharp cost increases**

"Extreme" BMI patients generate the highest spending.

### **4. Southeast region shows the highest averages**

### ⭐ **Main Insight**

> The most expensive segment is: **Smokers + High BMI + Age 50+ in the
> Southeast region**.

This is the key business insight the project reveals.

------------------------------------------------------------------------

# **6. Suggested Business Actions**

-   Provide smoking cessation incentives\
-   Create preventive health programs\
-   Adjust pricing tiers by age group\
-   Investigate regional health differences\
-   Encourage weight‑management initiatives

------------------------------------------------------------------------

# **7. Challenges**

-   Installing SQLite ODBC\
-   Understanding DW relationships\
-   Designing BMI & Age categories\
-   Being a beginner with OLAP & Power BI

------------------------------------------------------------------------

# **8. Ethical Considerations**

-   Avoid discriminatory pricing models\
-   Use data to improve health outcomes, not punish people\
-   Maintain transparency and privacy\
-   Ensure fairness in all analyses

------------------------------------------------------------------------

# **Git Commands**

    git add .
    git commit -m "Completed BI project with DW + OLAP + Storytelling"
    git push -u origin main
