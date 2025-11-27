Use silver;

— CRM - customer information 
—DDL

DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info 
( 
	cst_id INTEGER,
    cst_key VARCHAR(50), 
    cst_firstname VARCHAR(50), 
    cst_lastname VARCHAR(50), 
    cst_material_status VARCHAR(50), 
    cst_gndr VARCHAR(50), 
    cst_create_date DATE,
    dwh_create_date DATETIME DEFAULT NOW()
);

— LOADING

TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info
(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
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
    END AS cst_marital_status,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM
(
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC
        ) AS pk_rn
    FROM bronze.crm_cust_info -- reading from Bronze Database *****VERY IMPORTANT****
) t
WHERE pk_rn = 1
  AND cst_id <> 0;



— CRM Product info 

—DDL

DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info
(
		prd_id	INTEGER,
        cat_id VARCHAR(50),
		prd_key	VARCHAR(50),
		prd_nm	VARCHAR(50),
		prd_cost INTEGER,
		prd_line VARCHAR(50),
		prd_start_dt DATE,	
		prd_end_dt DATE,
        dwh_create_date DATETIME DEFAULT NOW()
);

— loading

TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info 
(
			prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt, 
            prd_end_dt
            
)
SELECT 
prd_id,
REPLACE(substring(prd_key,1,5),'-','_') as cat_id,
substring(prd_key,7,length(prd_key)) as prd_key,
prd_nm,
coalesce(prd_cost,0) as prd_cost,
CASE UPPER(TRIM(prd_line)) 
	WHEN  'M' THEN 'Mountain'
    WHEN  'R' THEN 'Road'
    WHEN  'S' THEN 'Other Sales'
    WHEN  'T' THEN 'Touring'
    ELSE 'n/a'
END as prd_line,
CAST(prd_start_dt as DATE) as prd_start_dt,
CAST(DATE_SUB(LEAD(prd_start_dt,1) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) , INTERVAL 1 DAY) as DATE) as prd_end_dt
FROM bronze.crm_prd_info; -- reading from Bronze Database *****VERY IMPORTANT****


SELECT * from crm_prd_info;


— CRM sales details 



-- Create Third Silver Layer Table : DDL (CRM Sales Details)
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE Table silver.crm_sales_details 
(
sls_ord_num	VARCHAR(50),
sls_prd_key	VARCHAR(50),
sls_cust_id	INTEGER,
sls_order_dt DATE,
sls_ship_dt	DATE,
sls_due_dt	DATE,
sls_sales	INTEGER,
sls_quantity INTEGER,
sls_price DECIMAL(10,2),
dwh_create_date DATETIME DEFAULT NOW()
);



-- cleaned sales details table.

INSERT INTO silver.crm_sales_details (
sls_ord_num	,
sls_prd_key	,
sls_cust_id	,
sls_order_dt ,
sls_ship_dt	,
sls_due_dt	,
sls_sales ,
sls_quantity ,
sls_price 
)
SELECT 
		sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        nullif(sls_order_dt,0) as sls_order_dt,
        nullif(sls_ship_dt,0) as sls_ship_dt,
        nullif(sls_due_dt,0) as sls_due_dt,
		CASE
				WHEN sls_sales IS NULL OR sls_sales <= 0 THEN
					ROUND(sls_quantity * 
								CASE
									WHEN sls_price IS NULL OR sls_price = 0 THEN
										sls_sales / NULLIF(sls_quantity, 0)
									ELSE ABS(sls_price)
								END)
				ELSE ROUND(sls_sales,2)
		END AS sls_sales,
        sls_quantity,
        CASE
        WHEN sls_price IS NULL OR sls_price = 0 THEN
            CASE 
                WHEN sls_quantity = 0 THEN 0
                ELSE ROUND(sls_sales / NULLIF(sls_quantity, 0),2)
            END
        ELSE ABS(sls_price)
    END AS sls_price
FROM bronze.crm_sales_details;

SELECT * FROM silver.crm_sales_details;


— ERP customer 

DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE Table silver.erp_cust_az12
(
cid	VARCHAR(50),
bdate	DATE,
gen VARCHAR(50),
dwh_create_date DATETIME DEFAULT NOW()
);



INSERT INTO silver.erp_cust_az12 (
cid,
bdate,
gen
)
SELECT 
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, length(cid))
            ELSE cid
		END as cid,
		CASE 
			WHEN bdate > now() THEN null
            ELSE bdate
        END as bdate,
         CASE 
		WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('M','MALE')
            THEN 'Male'
        WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('F','FEMALE')
            THEN 'Female'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;
-- Create Fifth Bronze Layer Table : DDL (ERP LOC_A101 )
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE Table silver.erp_loc_a101
(
cid	VARCHAR(50),
cntry VARCHAR(50),
dwh_create_date DATETIME DEFAULT NOW()
);

INSERT INTO silver.erp_loc_a101 (
cid,
cntry
)
SELECT 
		REPLACE(cid,'-','') as cid,
        CASE
		WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IN ('DE') THEN 'Germany'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IS NULL THEN 'n/a'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) = ''  THEN 'n/a'
        ELSE TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n',''))
	END as cntry
FROM bronze.erp_loc_a101;

SELECT * FROM silver.erp_loc_a101;

-- Create Sixth Bronze Layer Table : DDL (ERP PX_CAT_G1V2 )
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE Table silver.erp_px_cat_g1v2
(
id	VARCHAR(50),	
cat	VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(50),
dwh_create_date DATETIME DEFAULT NOW()
);


use silver;

INSERT INTO silver.erp_px_cat_g1v2(
id,
    cat,
    subcat,
    maintenance
)

SELECT 
	id,
    cat,
    subcat,
	REPLACE(REPLACE(maintenance,'\r',''),'\n','') as maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.erp_px_cat_g1v2;


——
-- SHOW VARIABLES LIKE 'local_infile';
-- SET GLOBAL local_infile = 1;
-- LOAD DATA LOCAL INFILE '/Users/lakshmirajendran/Documents/DWH Project/datasets/source_crm/cust_info.csv'
-- INTO TABLE crm_cust_info
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 LINES;


select * from crm_cust_info LIMIT 10;


select Count(cst_firstname) from crm_cust_info; -- 18494
desc crm_cust_info;

ALTER Table crm_cust_info RENAME COLUMN cst_is TO cst_id;

select * from crm_prd_info LIMIT 10;
select Count(*) from crm_prd_info; -- 397
desc crm_prd_info;

select * from crm_sales_details LIMIT 10;
select Count(*) from crm_sales_details; -- 60398

desc crm_sales_details;
 
SELECT COUNT(*) FROM erp_cust_az12; -- 18484

SELECT COUNT(*) FROM erp_loc_a101; -- 18484

SELECT COUNT(*) FROM erp_px_cat_g1v2; -- 37

--  Data Transformation and Data Cleansing
-- 1. to check the duplicate values
SELECT cst_id , COUNT(*) No_of_cust_id 
FROM crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- 2. Record 29466 duplicated 3 time 


SELECT * 
FROM crm_cust_info
WHERE cst_id = 29466;

-- to shortlist the one Record from the 3 Duplicated Cst_is 29466 by comparing the Latest Date.

SELECT * 
FROM 
		(SELECT cst_id,cst_create_Date,
				ROW_Number() OVER(PARTITION BY cst_id ORDER BY cst_create_Date DESC ) as last_date
		FROM crm_cust_info
        WHERE cst_id != 0)t
WHERE last_date = 1;  --  to filter only unique Records

-- WHERE cst_id = 29466;


-- Check for unwanted space
-- not the Result


SELECT * 
FROM crm_cust_info;
SELECT cst_lastname
FROM crm_cust_info
WHERE cst_lastname = TRIM(cst_lastname);

--

SELECT * FROM silver.crm_cust_info
;
SELECT cst_id, COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info
(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
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
    END AS cst_marital_status,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM
(
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC
        ) AS pk_rn
    FROM bronze.crm_cust_info -- reading from Bronze Database *****VERY IMPORTANT****
) t
WHERE pk_rn = 1
  AND cst_id <> 0;


-- Quality Check in Bronze Layer
-- 1. To detect Unique Primary Keys
SELECT * FROM crm_cust_info;

SELECT * 
FROM
(SELECT cst_id,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as pk_rn
FROM crm_cust_info)t
WHERE pk_rn = 1 AND cst_id != 0;

-- check for unwanted space
-- Expectation : no result (0 rows) 

SELECT cst_firstname, cst_lastname
FROM crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) OR cst_lastname != TRIM(cst_lastname); -- result have the extra space values

SELECT cst_gndr , length(cst_gndr) 
FROM crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr); -- output must be clear

-- Data Standardization And Consistency:

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;


-- cleaned Pro_info

SELECT 
prd_id,
prd_key,
substring(prd_key,1,5) as cat_id,
substr(prd_key,7, length(prd_key)) as prd_key1,
prd_nm,
coalesce(prd_cost,0) as prd_cost,
CASE UPPER(TRIM(prd_line)) 
	WHEN  'M' THEN 'Mountain'
    WHEN  'R' THEN 'Road'
    WHEN  'S' THEN 'Other Sales'
    WHEN  'T' THEN 'Touring'
    ELSE 'n/a'
END as prd_line,
CAST(prd_start_dt as DATE) as prd_start_dt,
CAST(DATE_SUB(LEAD(prd_start_dt,1) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) , INTERVAL 1 DAY) as DATE) as prd_end_dt
FROM crm_prd_info;

-- -- Check in Silver Layer:  crm_prd_info table

SELECT * 
FROM crm_prd_info; -- prd_id, prd_key, prd_nm, prd_cost, prd_inline, prd_start_dt, prd_end_dt

-- No of Records in Product Info Table

SELECT COUNT(*) 
FROM crm_prd_info; -- 397

-- To detect Duplicates Primary Keys

SELECT prd_id,COUNT(*) 
FROM crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL ;

-- To detect spaces prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt

SELECT prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
FROM crm_prd_info
WHERE prd_line != TRIM(prd_line);

-- To detect NULL and Negative Numbers from prd_cost

SELECT prd_cost
FROM crm_prd_info
WHERE prd_cost <= 0 OR prd_cost IS NULL;

-- Data Standardization and Consistency

SELECT DISTINCT prd_line
FROM crm_prd_info
WHERE prd_line IS NULL;

-- Check invalid Dates

SELECT * 
FROM crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- end date is Less than Starting Data checks

SELECT * 
FROM(SELECT 
prd_id,
prd_key,
substring(prd_key,1,5) as cat_id,
substr(prd_key,7, length(prd_key)) as prd_key1,
prd_nm,
coalesce(prd_cost,0) as prd_cost,
CASE UPPER(TRIM(prd_line)) 
	WHEN  'M' THEN 'Mountain'
    WHEN  'R' THEN 'Road'
    WHEN  'S' THEN 'Other Sales'
    WHEN  'T' THEN 'Touring'
    ELSE 'n/a'
END as prd_line,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt,1) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) as next_date
FROM crm_prd_info)t
WHERE next_date < prd_start_dt;


-- Clean and Load erp_cust_az12

SELECT * FROM silver.erp_cust_az12
WHERE CID LIKE '%AW00011002%';

SELECT Count(*) FROM silver.erp_cust_az12;

-- Compare the Column of 2 tables: Crm_cust_info.cst_key with erp_cust_az12.cid
SELECT count(*)
FROM silver.erp_cust_az12
WHERE cid IN (SELECT cst_key FROM silver.crm_cust_info);

INSERT INTO 
SELECT 
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, length(cid))
            ELSE cid
		END as cid_new,
		CASE 
			WHEN bdate > now() THEN null
            ELSE bdate
        END as bdate,
         CASE 
		WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('M','MALE')
            THEN 'Male'
        WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('F','FEMALE')
            THEN 'Female'
        ELSE 'n/a'
    END AS gen_new
FROM bronze.erp_cust_az12;

SELECT COUNT(DISTINCT gen)
FROM silver.erp_cust_az12;

SELECT  Distinct 
	gen,
	CASE 
			WHEN upper(trim(gen)) IN ('M','MALE') THEN 'Male'
            WHEN upper(trim(gen)) IN ('F','FEMALE') THEN 'Female'
            ELSE 'n/a'
	END as gen_new
FROM silver.erp_cust_az12;

use silver;

SELECT DISTINCT 
    gen,
    CASE 
		WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('M','MALE')
            THEN 'Male'
        WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('F','FEMALE')
            THEN 'Female'
        ELSE 'n/a'
    END AS gen_new
FROM bronze.erp_cust_az12;


— ERP location


— create 

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE Table silver.erp_loc_a101
(
cid	VARCHAR(50),
cntry VARCHAR(50),
dwh_create_date DATETIME DEFAULT NOW()
);

— load

INSERT INTO silver.erp_loc_a101 (
cid,
cntry
)
SELECT 
		REPLACE(cid,'-','') as cid,
        CASE
		WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IN ('DE') THEN 'Germany'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IS NULL THEN 'n/a'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) = ''  THEN 'n/a'
        ELSE TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n',''))
	END as cntry
FROM bronze.erp_loc_a101;



SELECT * FROM silver.erp_loc_a101;


— ERP customer category

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE Table silver.erp_px_cat_g1v2
(
id	VARCHAR(50),	
cat	VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(50),
dwh_create_date DATETIME DEFAULT NOW()
);

— LOAD

INSERT INTO silver.erp_px_cat_g1v2(
id,
    cat,
    subcat,
    maintenance
)

SELECT 
	id,
    cat,
    subcat,
	REPLACE(REPLACE(maintenance,'\r',''),'\n','') as maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.erp_px_cat_g1v2;


Quality checks


-- check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE MAINTENANCE != TRIM(MAINTENANCE) OR CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT);



-- Data Standardization and Consistenct
SELECT * FROM(SELECT 
id,
    cat,
    subcat,
REPLACE(REPLACE(MAINTENANCE,'\r',''),'\n','') as maintenance
FROM bronze.erp_px_cat_g1v2)t 
WHERE maintenance not in ('yes', 'no');


-- Quality Check in Bronze Layer
-- 1. To detect Unique Primary Keys
SELECT * FROM crm_cust_info;

SELECT * 
FROM
(SELECT cst_id,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as pk_rn
FROM crm_cust_info)t
WHERE pk_rn = 1 AND cst_id != 0;

-- check for unwanted space
-- Expectation : no result (0 rows) 

SELECT cst_firstname, cst_lastname
FROM crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) OR cst_lastname != TRIM(cst_lastname); -- result have the extra space values

SELECT cst_gndr , length(cst_gndr) 
FROM crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr); -- output must be clear

-- Data Standardization And Consistency:

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;
-- Clean crm_prd_info table

SELECT * 
FROM crm_prd_info; -- prd_id, prd_key, prd_nm, prd_cost, prd_inline, prd_start_dt, prd_end_dt

-- No of Records in Product Info Table

SELECT COUNT(*) 
FROM crm_prd_info; -- 397

-- To detect Duplicates Primary Keys

SELECT prd_id,COUNT(*) 
FROM crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL ;

-- To detect spaces prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt

SELECT prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
FROM crm_prd_info
WHERE prd_line != TRIM(prd_line);

-- To detect NULL and Negative Numbers from prd_cost

SELECT prd_cost
FROM crm_prd_info
WHERE prd_cost <= 0 OR prd_cost IS NULL;

-- Data Standardization and Consistency

SELECT DISTINCT prd_line
FROM crm_prd_info
WHERE prd_line IS NULL;

-- Check invalid Dates

SELECT * 
FROM crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- end date is Less than Starting Data checks

SELECT * 
FROM(SELECT 
prd_id,
prd_key,
substring(prd_key,1,5) as cat_id,
substr(prd_key,7, length(prd_key)) as prd_key1,
prd_nm,
coalesce(prd_cost,0) as prd_cost,
CASE UPPER(TRIM(prd_line)) 
	WHEN  'M' THEN 'Mountain'
    WHEN  'R' THEN 'Road'
    WHEN  'S' THEN 'Other Sales'
    WHEN  'T' THEN 'Touring'
    ELSE 'n/a'
END as prd_line,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt,1) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) as next_date
FROM crm_prd_info)t
WHERE next_date < prd_start_dt;
-- Clean the Third Table in CRM Sales Details

SELECT * from bronze.crm_sales_details; 
-- sls_ord_num , sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt,sls_due_dt, sls_sales, sls_quantity, sls_price

SELECT sls_ord_num , sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt,sls_due_dt, sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details;

-- Remove Duplicates using Primary Key
-- No need for this column bcoz prd_key and cst_id both the Primary Keys are belongs to another Table

-- date checks
SELECT * ,
nullif(sls_order_dt,0) as saleDt
FROM crm_sales_details
WHERE sls_order_dt <= 0 OR length(sls_order_dt) != 10 OR sls_order_dt > curDate() OR sls_order_dt < 2000-01-02 ;

SELECT *
FROM crm_sales_details
WHERE sls_ship_dt <= 0;

SELECT *
FROM crm_sales_details
WHERE sls_due_dt <= 0;
-- date outliner

SELECT * ,
nullif(sls_order_dt,0) as saleDt
FROM crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

SELECT * ,
nullif(sls_order_dt,0) as saleDt
FROM crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_ship_dt > sls_due_dt;

-- Business Ruls for Sales , Price and Quantity
-- Sales = Quantity * Price
--  No negative Values or Zeros in any Columns

--  No negative Values or Zeros in any Columns

SELECT 
	    sls_sales, 
        sls_quantity, 
        sls_price
FROM bronze.crm_sales_details
WHERE sls_sales <= 0 OR sls_sales IS NULL;

--  No negative Values or Zeros in any Columns

SELECT 
	    sls_sales, 
        sls_quantity, 
        sls_price
FROM bronze.crm_sales_details
WHERE sls_quantity <= 0 OR sls_quantity IS NULL;

--  No negative Values or Zeros in any Columns

SELECT 
	    sls_sales, 
        sls_quantity, 
        sls_price
FROM bronze.crm_sales_details
WHERE sls_price <= 0 OR sls_price IS NULL;

 -- to check where is not matching with Sales = Quantity * Price
SELECT 
		sls_sales as old_sls_sales,
        CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN  sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END as sls_sales,
        sls_quantity, 
		sls_price as old_sls_price,
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0  THEN  sls_sales/ Nullif(sls_quantity, 0 )  
			ELSE sls_price
		END as sls_price
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price);

-- If sales is negative or zero or null or Sales! = Quantity * Price then drive it using Sales = Quantity * Price
-- If Price is Negative then Convert to Positive Value.: Sql Function ABS() it will convert the negative value to positive
-- If price is zero or null or price !=Sales/Quantity  then, calculate it using Sales and Quantity.

-- cleaned sales details tanble.
SELECT 
		sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        nullif(sls_order_dt,0) as sls_order_dt,
        nullif(sls_ship_dt,0) as sls_ship_dt,
        nullif(sls_due_dt,0) as sls_due_dt,
		CASE
				WHEN sls_sales IS NULL OR sls_sales <= 0 THEN
					ROUND(sls_quantity * 
								CASE
									WHEN sls_price IS NULL OR sls_price = 0 THEN
										sls_sales / NULLIF(sls_quantity, 0)
									ELSE ABS(sls_price)
								END)
				ELSE ROUND(sls_sales,2)
		END AS sls_sales,
        sls_quantity,
        CASE
        WHEN sls_price IS NULL OR sls_price = 0 THEN
            CASE 
                WHEN sls_quantity = 0 THEN 0
                ELSE ROUND(sls_sales / NULLIF(sls_quantity, 0),2)
            END
        ELSE ABS(sls_price)
    END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales <=0 OR sls_sales IS NULL 
OR sls_price <= 0 OR sls_price IS NULL 
OR sls_quantity <= 0 OR sls_quantity IS NULL;		


SELECT *
FROM(SELECT 
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, length(cid))
            ELSE cid
		END as cid,
		CASE 
			WHEN bdate > now() THEN null
            ELSE bdate
        END as bdate,
         CASE 
		WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('M','MALE')
            THEN 'Male'
        WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('F','FEMALE')
            THEN 'Female'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12)t
WHERE bdate > NOW();
-- WHERE cid LIKE 'NAS%';
-- WHERE gen !=  'Male' AND gen != 'Female' AND gen != 'n/a' ;

-- WHERE cid LIKE 'NAS%' OR bdate > NOW() OR gen != 'Male' or gen != 'Female';

-- clean erp_loc_a101

SELECT * FROM (SELECT cid ,
	REPLACE(cid,'-','') as cid_new
FROM bronze.erp_loc_a101)t 
WHERE cid_new  NOT IN (SELECT cst_key FROM bronze.crm_cust_info);



SELECT * FROM (SELECT 
		REPLACE(cid,'-','') as cid,
        CASE
		WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IN ('DE') THEN 'Germany'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IS NULL THEN 'n/a'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) = ''  THEN 'n/a'
        ELSE TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n',''))
	END as cntry
FROM bronze.erp_loc_a101)t  
WHERE cntry IN('US','USA','DE','');
-- WHERE cid  NOT IN (SELECT cst_key FROM bronze.crm_cust_info);



SELECT DISTINCT cntry,
	CASE
		WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IN ('DE') THEN 'Germany'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IS NULL THEN 'n/a'
        WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) = ''  THEN 'n/a'
        ELSE TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n',''))
	END as cntry_new
FROM bronze.erp_loc_a101;


