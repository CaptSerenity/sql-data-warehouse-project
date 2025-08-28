/*
====================================================================================
Stored Procedure: Load Bronze Layer ( Source -> Bronze)
====================================================================================
Script Purpose: 
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Usese the 'BULK INSERT' command to laod data from csv files to bronze tables

Parameters: 
  None.
This is stored procedure does not accept any parameters or return any values.


Usage Example:
  EXEC bronze.load_bronze;
=====================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_time_start DATETIME, @batch_time_end DATETIME
	BEGIN TRY
		/*
		 Practicing Full Load to refresh and load the table 
		*/
		SET @batch_time_start = GETDATE();
		Print'==============================';
		Print 'Loading Bronze Layer';
		Print'==============================';

		Print '--------------------------';
		Print 'Loading CRM Tables';
		Print '--------------------------';


		SET @start_time = GETDATE();
		Print '>>Truncating Table:  bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info

		Print '>>Inserting Data Into:  bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\Python\DataWarehouse proj\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------';


		SET @start_time = GETDATE();
		Print '>>Truncating Table:  bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info

		Print '>>Inserting Data Into:  bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\Python\DataWarehouse proj\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------';


		SET @start_time = GETDATE();
		Print '>>Truncating Table:  bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details

		Print '>>Inserting Data Into:  bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\Python\DataWarehouse proj\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------';


		Print '--------------------------';
		Print 'Loading ERP Tables';
		Print '--------------------------';


		SET @start_time = GETDATE();
		Print '>>Truncating Table:  bronze.erp_loc_a101';
		TRUNCATE TABle bronze.erp_loc_a101

		Print '>>Inserting Data Into:  bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\Python\DataWarehouse proj\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------';



		SET @start_time = GETDATE();
		Print '>>Truncating Table:  bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12

		Print '>>Inserting Data Into:  bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\Python\DataWarehouse proj\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------';



		SET @start_time = GETDATE();
		Print '>>Truncating Table:  bronze.erm_px_cat_g1v2';
		TRUNCATE TABLE bronze.erm_px_cat_g1v2

		Print '>>Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\Python\DataWarehouse proj\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------';

		SET @batch_time_end = GETDATE();
		PRINT '=======================================';
		PRINT ' Loading Bronze Layer is Completed';
		PRINT '>>  Total Load Duration: ' + CAST(DATEDIFF(second, @batch_time_start, @batch_time_end) AS NVARCHAR) + ' seconds';
		PRINT '=======================================';
	END TRY
	BEGIN CATCH
		PRINT '======================='
		PRINT 'ERROR OCCURED DURING LOAD BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=======================';
	END CATCH
END
