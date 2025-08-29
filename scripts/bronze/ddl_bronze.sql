/*
===============================================================================
DDL SCRIPT: Create Bronze Layer
===============================================================================

Script Purpose:
  This script creates tables in the 'bronze' schema, dropping existing tables 
  if they already exist.
  Run this script to re-define the DDL structure of the 'bronze' tables

===============================================================================
*/


IF OBJECT_ID('bronze_crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO 

CREATE TABLE bronze.crm_cust_info ( 
    cst_id  INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);
GO


IF OBJECT_ID('bronze.crm_prd_info' , 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);
GO 

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARHCAR(50),
    sls_prod_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_date DATE,
    sls_ship_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);
GO

IF OBJECT_ID('bronze.erp_cust_az12' 'U') IS NOT NULL
    DROP TABLE 'bronze.erp_cust_az12';
GO

CREATE TABLE bronze.erp_cust_az12 ( 
    CID NVARCHAR(50),
    BDATE DATE,
    GEN NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_loc_a101' , 'U') IS NOT NULL
    DROP TABLE 'bronze.erp_loc_a101';
GO

CREATE TABLE bronze.erp_loc_a101
    CID NVARHCAR(50),
    CNTRY NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE 'bronze.erp_px_cat_g1v2';
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    ID NVARCHAR(50),
    CAT NVARCHAR(50),
    SUBCAT NVARCHAR(50),
    MAINTENANCE NVARCHAR(50)
);
GO
