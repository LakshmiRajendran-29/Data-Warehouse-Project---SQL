import mysql.connector
import time

# ============================================
# 1. Database Connection
# ============================================
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="786sunflower",
    database="silver",
    allow_local_infile=True
)

cursor = conn.cursor()
print("\n================ Silver Layer Load Started ================\n")
batch_start = time.time()


# ============================================
# 2. Helper Function to Execute SQL Blocks
# ============================================
def run_sql_block(name, sql_text):
    print(f"\n------------ Loading {name} -------------")
    start = time.time()

    try:
        for stmt in sql_text.strip().split(";"):
            if stmt.strip():
                cursor.execute(stmt)

        conn.commit()
        print(f">> {name} Loaded Successfully ({round(time.time() - start, 2)} seconds)")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ ERROR loading {name}: {e}\n")
        raise e


# ============================================
# 3. SQL Blocks for Each Table
# ============================================

# ---------------- CRM Customer -----------------
crm_cust_sql = """
DROP TABLE IF EXISTS crm_cust_info;
CREATE TABLE crm_cust_info 
( 
    cst_id INTEGER,
    cst_key VARCHAR(50), 
    cst_firstname VARCHAR(50), 
    cst_lastname VARCHAR(50), 
    cst_marital_status VARCHAR(50), 
    cst_gndr VARCHAR(50), 
    cst_create_date DATE,
    dwh_create_date DATETIME DEFAULT NOW()
);

INSERT INTO crm_cust_info (
    cst_id, cst_key, cst_firstname, cst_lastname,
    cst_marital_status, cst_gndr, cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        ELSE 'n/a'
    END,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a'
    END,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS pk_rn
    FROM bronze.crm_cust_info
) t
WHERE pk_rn = 1 AND cst_id <> 0;
"""


# ---------------- CRM Product -----------------
crm_prd_sql = """
DROP TABLE IF EXISTS crm_prd_info;
CREATE TABLE crm_prd_info
(
    prd_id INTEGER,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INTEGER,
    prd_line VARCHAR(50),
    prd_start_dt DATE,    
    prd_end_dt DATE,
    dwh_create_date DATETIME DEFAULT NOW()
);

INSERT INTO crm_prd_info (
    prd_id, cat_id, prd_key, prd_nm,
    prd_cost, prd_line, prd_start_dt, prd_end_dt
)
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
    SUBSTRING(prd_key,7),
    prd_nm,
    COALESCE(prd_cost,0),
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END,
    CAST(prd_start_dt AS DATE),
    CAST(
        DATE_SUB(
            LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt),
            INTERVAL 1 DAY
        ) AS DATE
    )
FROM bronze.crm_prd_info;
"""


# ---------------- CRM Sales Details -----------------
crm_sales_sql = """
DROP TABLE IF EXISTS crm_sales_details;
CREATE TABLE crm_sales_details 
(
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INTEGER,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INTEGER,
    sls_quantity INTEGER,
    sls_price DECIMAL(10,2),
    dwh_create_date DATETIME DEFAULT NOW()
);

INSERT INTO crm_sales_details (
    sls_ord_num, sls_prd_key, sls_cust_id,
    sls_order_dt, sls_ship_dt, sls_due_dt,
    sls_sales, sls_quantity, sls_price
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    NULLIF(sls_order_dt,0),
    NULLIF(sls_ship_dt,0),
    NULLIF(sls_due_dt,0),

    CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 THEN
            ROUND(sls_quantity *
                 CASE
                     WHEN sls_price IS NULL OR sls_price = 0 THEN
                         sls_sales / NULLIF(sls_quantity, 0)
                     ELSE ABS(sls_price)
                 END)
        ELSE ROUND(sls_sales,2)
    END,

    sls_quantity,

    CASE
        WHEN sls_price IS NULL OR sls_price = 0 THEN
            CASE WHEN sls_quantity = 0 THEN 0
                 ELSE ROUND(sls_sales / NULLIF(sls_quantity, 0),2)
            END
        ELSE ABS(sls_price)
    END
FROM bronze.crm_sales_details;
"""


# ---------------- ERP Customer -----------------
erp_cust_sql = """
DROP TABLE IF EXISTS erp_cust_az12;
CREATE TABLE erp_cust_az12
(
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50),
    dwh_create_date DATETIME DEFAULT NOW()
);

INSERT INTO erp_cust_az12 (cid, bdate, gen)
SELECT 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END,
    CASE WHEN bdate > NOW() THEN NULL ELSE bdate END,
    CASE
        WHEN UPPER(TRIM(REPLACE(REPLACE(gen,'\\r',''), '\\n',''))) IN ('M','MALE') THEN 'Male'
        WHEN UPPER(TRIM(REPLACE(REPLACE(gen,'\\r',''), '\\n',''))) IN ('F','FEMALE') THEN 'Female'
        ELSE 'n/a'
    END
FROM bronze.erp_cust_az12;
"""


# ---------------- ERP LOC -----------------
erp_loc_sql = """
DROP TABLE IF EXISTS erp_loc_a101;
CREATE TABLE erp_loc_a101
(
    cid VARCHAR(50),
    cntry VARCHAR(50),
    dwh_create_date DATETIME DEFAULT NOW()
);

INSERT INTO erp_loc_a101 (cid, cntry)
SELECT 
    REPLACE(cid,'-',''),
    CASE 
        WHEN TRIM(REPLACE(REPLACE(cntry,'\\r',''), '\\n','')) IN ('US','USA') THEN 'United States'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\\r',''), '\\n','')) = 'DE' THEN 'Germany'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\\r',''), '\\n','')) IN ('', NULL) THEN 'n/a'
        ELSE TRIM(REPLACE(REPLACE(cntry,'\\r',''), '\\n',''))
    END
FROM bronze.erp_loc_a101;
"""


# ---------------- ERP PX -----------------
erp_px_sql = """
DROP TABLE IF EXISTS erp_px_cat_g1v2;
CREATE TABLE erp_px_cat_g1v2
(
    id VARCHAR(50),    
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50),
    dwh_create_date DATETIME DEFAULT NOW()
);

INSERT INTO erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT 
    id, cat, subcat,
    REPLACE(REPLACE(maintenance,'\\r',''), '\\n','')
FROM bronze.erp_px_cat_g1v2;
"""


# ============================================
# 4. Execute All Loads
# ============================================
run_sql_block("CRM Customer", crm_cust_sql)
run_sql_block("CRM Product", crm_prd_sql)
run_sql_block("CRM Sales Details", crm_sales_sql)
run_sql_block("ERP Customer", erp_cust_sql)
run_sql_block("ERP Location", erp_loc_sql)
run_sql_block("ERP PX Category", erp_px_sql)

# ============================================
# 5. Done
# ============================================
print("\n================ Silver Layer Load Completed ================")
print(f"Total Time: {round(time.time() - batch_start, 2)} seconds\n")

cursor.close()
conn.close()
