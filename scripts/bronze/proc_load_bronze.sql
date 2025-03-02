/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
EXEC bronze.load_bronze
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
    DECLARE @start_time DATETIME,
            @end_time DATETIME,
            @bronze_start DATETIME,
            @bronze_end DATETIME;
			 
    BEGIN TRY
        SET @bronze_start = GETDATE();  
        PRINT '=====================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '=====================================================';

        PRINT '-----------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-----------------------------------------------------';

        -- Customer Info Table
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
            TRUNCATE TABLE bronze.crm_cust_info;
    
        PRINT '>> Inserting data into bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\User\OneDrive - MSFT\My Files\MY PROJECTS\04 My Repo\Datawarehousing\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK 
        );
        SET @end_time = GETDATE();
        PRINT '=====================================================';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '=====================================================';

        -- Product Info Table
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
            TRUNCATE TABLE bronze.crm_prd_info;
    
        PRINT '>> Inserting data into bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\User\OneDrive - MSFT\My Files\MY PROJECTS\04 My Repo\Datawarehousing\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK 
        );
        SET @end_time = GETDATE();
        PRINT '=====================================================';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '=====================================================';

        -- Sales Details Table
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
            TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting data into bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\User\OneDrive - MSFT\My Files\MY PROJECTS\04 My Repo\Datawarehousing\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK 
        );
        SET @end_time = GETDATE();
        PRINT '=====================================================';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '=====================================================';

        PRINT '-----------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-----------------------------------------------------';

        -- ERP Customer AZ12 Table
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
            TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting data into bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\User\OneDrive - MSFT\My Files\MY PROJECTS\04 My Repo\Datawarehousing\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK 
        );
        SET @end_time = GETDATE();
        PRINT '=====================================================';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '=====================================================';

        -- ERP Location A101 Table
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
            TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting data into bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\User\OneDrive - MSFT\My Files\MY PROJECTS\04 My Repo\Datawarehousing\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK 
        );
        SET @end_time = GETDATE();
        PRINT '=====================================================';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '=====================================================';

        -- ERP Product Category G1V2 Table
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'; 
        IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
            TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting data into bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\User\OneDrive - MSFT\My Files\MY PROJECTS\04 My Repo\Datawarehousing\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK 
        );
        SET @end_time = GETDATE();
        PRINT '=====================================================';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '=====================================================';

        SET @bronze_end = GETDATE();
        PRINT '=====================================================';
        PRINT '>> Total Duration to load bronze layer: ' + CAST(DATEDIFF(SECOND, @bronze_start, @bronze_end) AS NVARCHAR(10)) + ' seconds';
        PRINT '=====================================================';
    END TRY
    BEGIN CATCH
        PRINT '=====================================================';
        PRINT 'Error Occurred During Loading Bronze Layer';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT '=====================================================';
        THROW;
    END CATCH
END;
