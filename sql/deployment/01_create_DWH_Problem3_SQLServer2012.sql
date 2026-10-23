/*
===============================================================================
01_create_DWH_Problem3_SQLServer2012.sql
Project : Problem 3 - Customer Behavior & Customer Profile
Target  : Microsoft SQL Server 2012
Database: DWH_Problem3

Architecture
------------
core : 14 normalized 3NF relations
dw   : Star/Fact Constellation Data Warehouse (7 dimensions, 3 facts, 1 bridge)
gold : Problem 3 Customer Analytics Data Mart (2 facts, 5 marts)

Notes
-----
- SQL Server 2012 compatible: no DROP TABLE IF EXISTS, CREATE OR ALTER, STRING_AGG, OPENJSON.
- Logical BOOLEAN fields are stored as BIT.
- Logical VARCHAR text is stored as NVARCHAR for Unicode safety.
- Gold V2 loaded here preserves ALL source order statuses. The official analytical
  scope (delivered, paid, shipped) is applied/recomputed in EDA V2; see validation
  section in 03_validate_DWH_Problem3_SQLServer2012.sql.
===============================================================================
*/

USE [master];
GO

IF DB_ID(N'DWH_Problem3') IS NULL
BEGIN
    PRINT N'Creating database DWH_Problem3...';
    CREATE DATABASE [DWH_Problem3];
END
ELSE
    PRINT N'Database DWH_Problem3 already exists.';
GO

USE [DWH_Problem3];
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'core')
    EXEC(N'CREATE SCHEMA [core] AUTHORIZATION [dbo]');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dw')
    EXEC(N'CREATE SCHEMA [dw] AUTHORIZATION [dbo]');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'gold')
    EXEC(N'CREATE SCHEMA [gold] AUTHORIZATION [dbo]');
GO

/* ============================================================================
   A. CORE 3NF
   ============================================================================ */

IF OBJECT_ID(N'[core].[GEOGRAPHY]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[GEOGRAPHY](
    [zip] NVARCHAR(20) NOT NULL,
    [city] NVARCHAR(100) NOT NULL,
    [district] NVARCHAR(100) NOT NULL,
    [region] NVARCHAR(100) NOT NULL,
    CONSTRAINT [PK_core_GEOGRAPHY] PRIMARY KEY CLUSTERED ([zip])
);
END
GO

IF OBJECT_ID(N'[core].[PRODUCTS]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[PRODUCTS](
    [product_id] BIGINT NOT NULL,
    [product_name] NVARCHAR(100) NOT NULL,
    [category] NVARCHAR(50) NOT NULL,
    [segment] NVARCHAR(50) NOT NULL,
    [size] NVARCHAR(20) NOT NULL,
    [color] NVARCHAR(20) NOT NULL,
    [price] DECIMAL(18,2) NOT NULL,
    [cogs] DECIMAL(18,2) NOT NULL,
    CONSTRAINT [PK_core_PRODUCTS] PRIMARY KEY CLUSTERED ([product_id])
);
END
GO

IF OBJECT_ID(N'[core].[PROMOTIONS]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[PROMOTIONS](
    [promo_id] NVARCHAR(20) NOT NULL,
    [promo_name] NVARCHAR(100) NOT NULL,
    [promo_type] NVARCHAR(30) NOT NULL,
    [discount_value] DECIMAL(18,2) NOT NULL,
    [start_date] DATE NOT NULL,
    [end_date] DATE NOT NULL,
    [applicable_category] NVARCHAR(50) NULL,
    [promo_channel] NVARCHAR(50) NOT NULL,
    [stackable_flag] BIT NOT NULL,
    [min_order_value] DECIMAL(18,2) NOT NULL,
    CONSTRAINT [PK_core_PROMOTIONS] PRIMARY KEY CLUSTERED ([promo_id])
);
END
GO

IF OBJECT_ID(N'[core].[SALES_EMPLOYEES]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[SALES_EMPLOYEES](
    [sales_employee_id] NVARCHAR(20) NOT NULL,
    [sales_employee_name] NVARCHAR(100) NOT NULL,
    [marital_status] NVARCHAR(20) NOT NULL,
    [education_level] NVARCHAR(50) NOT NULL,
    [years_experience] INT NOT NULL,
    CONSTRAINT [PK_core_SALES_EMPLOYEES] PRIMARY KEY CLUSTERED ([sales_employee_id])
);
END
GO

IF OBJECT_ID(N'[core].[SHIPPERS]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[SHIPPERS](
    [shipper_id] NVARCHAR(20) NOT NULL,
    [shipper_name] NVARCHAR(100) NOT NULL,
    [shipper_phone] NVARCHAR(20) NOT NULL,
    [shipper_gender] NVARCHAR(20) NOT NULL,
    [shipper_age] INT NOT NULL,
    [shipper_marital_status] NVARCHAR(20) NOT NULL,
    [shipper_education] NVARCHAR(50) NOT NULL,
    [shipper_company] NVARCHAR(100) NOT NULL,
    [shipper_vehicle] NVARCHAR(50) NOT NULL,
    [shipper_experience_years] INT NOT NULL,
    [shipper_rating] DECIMAL(3,2) NOT NULL,
    [delivery_success_rate] DECIMAL(5,2) NOT NULL,
    [average_delivery_time] DECIMAL(10,2) NOT NULL,
    [working_shift] NVARCHAR(50) NOT NULL,
    [join_date] DATE NOT NULL,
    [city] NVARCHAR(100) NOT NULL,
    [region] NVARCHAR(100) NOT NULL,
    [district] NVARCHAR(100) NOT NULL,
    CONSTRAINT [PK_core_SHIPPERS] PRIMARY KEY CLUSTERED ([shipper_id])
);
END
GO

IF OBJECT_ID(N'[core].[CUSTOMERS]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[CUSTOMERS](
    [customer_id] BIGINT NOT NULL,
    [zip] NVARCHAR(20) NOT NULL,
    [signup_date] DATE NOT NULL,
    [gender] NVARCHAR(20) NOT NULL,
    [age_group] NVARCHAR(20) NOT NULL,
    [acquisition_channel] NVARCHAR(50) NOT NULL,
    CONSTRAINT [PK_core_CUSTOMERS] PRIMARY KEY CLUSTERED ([customer_id]),
    CONSTRAINT [FK_core_CUSTOMERS_GEOGRAPHY]
        FOREIGN KEY ([zip]) REFERENCES [core].[GEOGRAPHY]([zip])
);
END
GO

IF OBJECT_ID(N'[core].[ORDERS]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[ORDERS](
    [order_id] BIGINT NOT NULL,
    [customer_id] BIGINT NOT NULL,
    [sales_employee_id] NVARCHAR(20) NOT NULL,
    [order_date] DATE NOT NULL,
    [order_status] NVARCHAR(30) NOT NULL,
    [device_type] NVARCHAR(30) NOT NULL,
    [order_source] NVARCHAR(50) NOT NULL,
    [comment] NVARCHAR(255) NULL,
    CONSTRAINT [PK_core_ORDERS] PRIMARY KEY CLUSTERED ([order_id]),
    CONSTRAINT [FK_core_ORDERS_CUSTOMERS]
        FOREIGN KEY ([customer_id]) REFERENCES [core].[CUSTOMERS]([customer_id]),
    CONSTRAINT [FK_core_ORDERS_SALES_EMPLOYEES]
        FOREIGN KEY ([sales_employee_id]) REFERENCES [core].[SALES_EMPLOYEES]([sales_employee_id])
);
END
GO

IF OBJECT_ID(N'[core].[ORDER_ITEMS]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[ORDER_ITEMS](
    [order_item_id] BIGINT NOT NULL,
    [order_id] BIGINT NOT NULL,
    [product_id] BIGINT NOT NULL,
    [quantity] INT NOT NULL,
    [unit_price] DECIMAL(18,2) NOT NULL,
    [discount_amount] DECIMAL(18,2) NOT NULL,
    CONSTRAINT [PK_core_ORDER_ITEMS] PRIMARY KEY CLUSTERED ([order_item_id]),
    CONSTRAINT [FK_core_ORDER_ITEMS_ORDERS]
        FOREIGN KEY ([order_id]) REFERENCES [core].[ORDERS]([order_id]),
    CONSTRAINT [FK_core_ORDER_ITEMS_PRODUCTS]
        FOREIGN KEY ([product_id]) REFERENCES [core].[PRODUCTS]([product_id])
);
END
GO

IF OBJECT_ID(N'[core].[ORDER_ITEM_PROMOTION]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[ORDER_ITEM_PROMOTION](
    [order_item_id] BIGINT NOT NULL,
    [promo_id] NVARCHAR(20) NOT NULL,
    [promotion_order] SMALLINT NOT NULL,
    CONSTRAINT [PK_core_ORDER_ITEM_PROMOTION]
        PRIMARY KEY CLUSTERED ([order_item_id], [promo_id]),
    CONSTRAINT [UQ_core_ORDER_ITEM_PROMOTION_order]
        UNIQUE ([order_item_id], [promotion_order]),
    CONSTRAINT [CK_core_ORDER_ITEM_PROMOTION_order]
        CHECK ([promotion_order] IN (1,2)),
    CONSTRAINT [FK_core_ORDER_ITEM_PROMOTION_ITEM]
        FOREIGN KEY ([order_item_id]) REFERENCES [core].[ORDER_ITEMS]([order_item_id]),
    CONSTRAINT [FK_core_ORDER_ITEM_PROMOTION_PROMO]
        FOREIGN KEY ([promo_id]) REFERENCES [core].[PROMOTIONS]([promo_id])
);
END
GO

IF OBJECT_ID(N'[core].[PAYMENTS]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[PAYMENTS](
    [order_id] BIGINT NOT NULL,
    [payment_method] NVARCHAR(30) NOT NULL,
    [payment_value] DECIMAL(18,2) NOT NULL,
    [installments] INT NOT NULL,
    CONSTRAINT [PK_core_PAYMENTS] PRIMARY KEY CLUSTERED ([order_id]),
    CONSTRAINT [FK_core_PAYMENTS_ORDERS]
        FOREIGN KEY ([order_id]) REFERENCES [core].[ORDERS]([order_id])
);
END
GO

IF OBJECT_ID(N'[core].[SHIPMENTS]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[SHIPMENTS](
    [order_id] BIGINT NOT NULL,
    [shipper_id] NVARCHAR(20) NOT NULL,
    [ship_date] DATE NOT NULL,
    [delivery_date] DATE NOT NULL,
    [shipping_fee] DECIMAL(18,2) NOT NULL,
    CONSTRAINT [PK_core_SHIPMENTS] PRIMARY KEY CLUSTERED ([order_id]),
    CONSTRAINT [FK_core_SHIPMENTS_ORDERS]
        FOREIGN KEY ([order_id]) REFERENCES [core].[ORDERS]([order_id]),
    CONSTRAINT [FK_core_SHIPMENTS_SHIPPERS]
        FOREIGN KEY ([shipper_id]) REFERENCES [core].[SHIPPERS]([shipper_id])
);
END
GO

IF OBJECT_ID(N'[core].[REVIEWS]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[REVIEWS](
    [review_id] NVARCHAR(20) NOT NULL,
    [order_id] BIGINT NOT NULL,
    [product_id] BIGINT NOT NULL,
    [review_date] DATE NOT NULL,
    [rating] INT NOT NULL,
    [review_title] NVARCHAR(255) NOT NULL,
    CONSTRAINT [PK_core_REVIEWS] PRIMARY KEY CLUSTERED ([review_id]),
    CONSTRAINT [FK_core_REVIEWS_ORDERS]
        FOREIGN KEY ([order_id]) REFERENCES [core].[ORDERS]([order_id]),
    CONSTRAINT [FK_core_REVIEWS_PRODUCTS]
        FOREIGN KEY ([product_id]) REFERENCES [core].[PRODUCTS]([product_id])
);
END
GO

IF OBJECT_ID(N'[core].[RETURNS]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[RETURNS](
    [return_id] NVARCHAR(20) NOT NULL,
    [order_id] BIGINT NOT NULL,
    [product_id] BIGINT NOT NULL,
    [return_date] DATE NOT NULL,
    [return_reason] NVARCHAR(100) NOT NULL,
    [return_quantity] INT NOT NULL,
    [refund_amount] DECIMAL(18,2) NOT NULL,
    CONSTRAINT [PK_core_RETURNS] PRIMARY KEY CLUSTERED ([return_id]),
    CONSTRAINT [FK_core_RETURNS_ORDERS]
        FOREIGN KEY ([order_id]) REFERENCES [core].[ORDERS]([order_id]),
    CONSTRAINT [FK_core_RETURNS_PRODUCTS]
        FOREIGN KEY ([product_id]) REFERENCES [core].[PRODUCTS]([product_id])
);
END
GO

IF OBJECT_ID(N'[core].[INVENTORY_SNAPSHOT]', N'U') IS NULL
BEGIN
CREATE TABLE [core].[INVENTORY_SNAPSHOT](
    [product_id] BIGINT NOT NULL,
    [snapshot_date] DATE NOT NULL,
    [stock_on_hand] INT NOT NULL,
    [units_received] INT NOT NULL,
    [units_sold] INT NOT NULL,
    [stockout_days] INT NOT NULL,
    [days_of_supply] DECIMAL(10,2) NOT NULL,
    [fill_rate] DECIMAL(5,4) NOT NULL,
    [stockout_flag] BIT NOT NULL,
    [overstock_flag] BIT NOT NULL,
    [reorder_flag] BIT NOT NULL,
    [sell_through_rate] DECIMAL(5,4) NOT NULL,
    CONSTRAINT [PK_core_INVENTORY_SNAPSHOT]
        PRIMARY KEY CLUSTERED ([product_id], [snapshot_date]),
    CONSTRAINT [FK_core_INVENTORY_SNAPSHOT_PRODUCTS]
        FOREIGN KEY ([product_id]) REFERENCES [core].[PRODUCTS]([product_id])
);
END
GO

/* Core indexes */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_core_ORDERS_customer_date' AND object_id=OBJECT_ID(N'[core].[ORDERS]'))
    CREATE NONCLUSTERED INDEX [IX_core_ORDERS_customer_date] ON [core].[ORDERS]([customer_id],[order_date]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_core_ORDERS_status_date' AND object_id=OBJECT_ID(N'[core].[ORDERS]'))
    CREATE NONCLUSTERED INDEX [IX_core_ORDERS_status_date] ON [core].[ORDERS]([order_status],[order_date]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_core_ORDER_ITEMS_order' AND object_id=OBJECT_ID(N'[core].[ORDER_ITEMS]'))
    CREATE NONCLUSTERED INDEX [IX_core_ORDER_ITEMS_order] ON [core].[ORDER_ITEMS]([order_id]) INCLUDE([product_id],[quantity],[unit_price],[discount_amount]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_core_ORDER_ITEMS_product' AND object_id=OBJECT_ID(N'[core].[ORDER_ITEMS]'))
    CREATE NONCLUSTERED INDEX [IX_core_ORDER_ITEMS_product] ON [core].[ORDER_ITEMS]([product_id]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_core_SHIPMENTS_shipper' AND object_id=OBJECT_ID(N'[core].[SHIPMENTS]'))
    CREATE NONCLUSTERED INDEX [IX_core_SHIPMENTS_shipper] ON [core].[SHIPMENTS]([shipper_id]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_core_REVIEWS_order_product' AND object_id=OBJECT_ID(N'[core].[REVIEWS]'))
    CREATE NONCLUSTERED INDEX [IX_core_REVIEWS_order_product] ON [core].[REVIEWS]([order_id],[product_id]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_core_RETURNS_order_product' AND object_id=OBJECT_ID(N'[core].[RETURNS]'))
    CREATE NONCLUSTERED INDEX [IX_core_RETURNS_order_product] ON [core].[RETURNS]([order_id],[product_id]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_core_INVENTORY_snapshot_date' AND object_id=OBJECT_ID(N'[core].[INVENTORY_SNAPSHOT]'))
    CREATE NONCLUSTERED INDEX [IX_core_INVENTORY_snapshot_date] ON [core].[INVENTORY_SNAPSHOT]([snapshot_date]);
GO

/* ============================================================================
   B. DATA WAREHOUSE
   ============================================================================ */

IF OBJECT_ID(N'[dw].[DIM_DATE]', N'U') IS NULL
BEGIN
CREATE TABLE [dw].[DIM_DATE](
    [date_key] INT NOT NULL,
    [full_date] DATE NOT NULL,
    [day] SMALLINT NOT NULL,
    [day_name] NVARCHAR(12) NOT NULL,
    [day_of_week] TINYINT NOT NULL,
    [week_of_year] SMALLINT NOT NULL,
    [month] TINYINT NOT NULL,
    [month_name] NVARCHAR(12) NOT NULL,
    [quarter] TINYINT NOT NULL,
    [year] SMALLINT NOT NULL,
    [year_month] NCHAR(7) NOT NULL,
    [is_weekend] BIT NOT NULL,
    CONSTRAINT [PK_dw_DIM_DATE] PRIMARY KEY CLUSTERED ([date_key]),
    CONSTRAINT [UQ_dw_DIM_DATE_full_date] UNIQUE ([full_date]),
    CONSTRAINT [CK_dw_DIM_DATE_dow] CHECK ([day_of_week] BETWEEN 1 AND 7),
    CONSTRAINT [CK_dw_DIM_DATE_month] CHECK ([month] BETWEEN 1 AND 12),
    CONSTRAINT [CK_dw_DIM_DATE_quarter] CHECK ([quarter] BETWEEN 1 AND 4)
);
END
GO

IF OBJECT_ID(N'[dw].[DIM_GEOGRAPHY]', N'U') IS NULL
BEGIN
CREATE TABLE [dw].[DIM_GEOGRAPHY](
    [geo_key] BIGINT NOT NULL,
    [zip] NVARCHAR(20) NOT NULL,
    [city] NVARCHAR(100) NOT NULL,
    [district] NVARCHAR(100) NOT NULL,
    [region] NVARCHAR(100) NOT NULL,
    CONSTRAINT [PK_dw_DIM_GEOGRAPHY] PRIMARY KEY CLUSTERED ([geo_key]),
    CONSTRAINT [UQ_dw_DIM_GEOGRAPHY_zip] UNIQUE ([zip])
);
END
GO

IF OBJECT_ID(N'[dw].[DIM_PRODUCT]', N'U') IS NULL
BEGIN
CREATE TABLE [dw].[DIM_PRODUCT](
    [product_key] BIGINT NOT NULL,
    [product_id] BIGINT NOT NULL,
    [product_name] NVARCHAR(255) NOT NULL,
    [category] NVARCHAR(50) NOT NULL,
    [segment] NVARCHAR(50) NOT NULL,
    [size] NVARCHAR(50) NOT NULL,
    [color] NVARCHAR(50) NOT NULL,
    [list_price] DECIMAL(18,2) NOT NULL,
    [cogs] DECIMAL(18,2) NOT NULL,
    [scd_version] INT NOT NULL,
    [is_current] BIT NOT NULL,
    CONSTRAINT [PK_dw_DIM_PRODUCT] PRIMARY KEY CLUSTERED ([product_key]),
    CONSTRAINT [UQ_dw_DIM_PRODUCT_product_id] UNIQUE ([product_id])
);
END
GO

IF OBJECT_ID(N'[dw].[DIM_PROMOTION]', N'U') IS NULL
BEGIN
CREATE TABLE [dw].[DIM_PROMOTION](
    [promotion_key] BIGINT NOT NULL,
    [promo_id] NVARCHAR(20) NOT NULL,
    [promo_name] NVARCHAR(255) NOT NULL,
    [promo_type] NVARCHAR(50) NOT NULL,
    [discount_value] DECIMAL(18,2) NOT NULL,
    [start_date] DATE NOT NULL,
    [end_date] DATE NOT NULL,
    [applicable_category] NVARCHAR(100) NULL,
    [promo_channel] NVARCHAR(50) NOT NULL,
    [stackable_flag] BIT NOT NULL,
    [min_order_value] DECIMAL(18,2) NOT NULL,
    CONSTRAINT [PK_dw_DIM_PROMOTION] PRIMARY KEY CLUSTERED ([promotion_key]),
    CONSTRAINT [UQ_dw_DIM_PROMOTION_promo_id] UNIQUE ([promo_id])
);
END
GO

IF OBJECT_ID(N'[dw].[DIM_CHANNEL]', N'U') IS NULL
BEGIN
CREATE TABLE [dw].[DIM_CHANNEL](
    [channel_key] BIGINT NOT NULL,
    [order_source] NVARCHAR(50) NOT NULL,
    [device_type] NVARCHAR(30) NOT NULL,
    [payment_method] NVARCHAR(50) NOT NULL,
    CONSTRAINT [PK_dw_DIM_CHANNEL] PRIMARY KEY CLUSTERED ([channel_key]),
    CONSTRAINT [UQ_dw_DIM_CHANNEL_combo] UNIQUE ([order_source],[device_type],[payment_method])
);
END
GO

IF OBJECT_ID(N'[dw].[DIM_ORDER_STATUS]', N'U') IS NULL
BEGIN
CREATE TABLE [dw].[DIM_ORDER_STATUS](
    [order_status_key] BIGINT NOT NULL,
    [order_status] NVARCHAR(30) NOT NULL,
    CONSTRAINT [PK_dw_DIM_ORDER_STATUS] PRIMARY KEY CLUSTERED ([order_status_key]),
    CONSTRAINT [UQ_dw_DIM_ORDER_STATUS_status] UNIQUE ([order_status])
);
END
GO

IF OBJECT_ID(N'[dw].[DIM_CUSTOMER]', N'U') IS NULL
BEGIN
CREATE TABLE [dw].[DIM_CUSTOMER](
    [customer_key] BIGINT NOT NULL,
    [customer_id] BIGINT NOT NULL,
    [zip] NVARCHAR(20) NOT NULL,
    [geo_key] BIGINT NOT NULL,
    [signup_date] DATE NOT NULL,
    [gender] NVARCHAR(20) NOT NULL,
    [age_group] NVARCHAR(20) NOT NULL,
    [acquisition_channel] NVARCHAR(50) NOT NULL,
    [scd_version] INT NOT NULL,
    [is_current] BIT NOT NULL,
    CONSTRAINT [PK_dw_DIM_CUSTOMER] PRIMARY KEY CLUSTERED ([customer_key]),
    CONSTRAINT [UQ_dw_DIM_CUSTOMER_customer_id] UNIQUE ([customer_id]),
    CONSTRAINT [FK_dw_DIM_CUSTOMER_GEOGRAPHY]
        FOREIGN KEY ([geo_key]) REFERENCES [dw].[DIM_GEOGRAPHY]([geo_key])
);
END
GO

IF OBJECT_ID(N'[dw].[FACT_SALES_LINE]', N'U') IS NULL
BEGIN
CREATE TABLE [dw].[FACT_SALES_LINE](
    [sales_line_key] BIGINT NOT NULL,
    [order_item_id] BIGINT NOT NULL,
    [order_id] BIGINT NOT NULL,
    [date_key] INT NOT NULL,
    [customer_key] BIGINT NOT NULL,
    [product_key] BIGINT NOT NULL,
    [geo_key] BIGINT NOT NULL,
    [channel_key] BIGINT NOT NULL,
    [order_status_key] BIGINT NOT NULL,
    [quantity] INT NOT NULL,
    [unit_price] DECIMAL(18,2) NOT NULL,
    [discount_amount] DECIMAL(18,2) NOT NULL,
    [gross_sales] DECIMAL(18,2) NOT NULL,
    [net_sales] DECIMAL(18,2) NOT NULL,
    CONSTRAINT [PK_dw_FACT_SALES_LINE] PRIMARY KEY CLUSTERED ([sales_line_key]),
    CONSTRAINT [UQ_dw_FACT_SALES_LINE_order_item_id] UNIQUE ([order_item_id]),
    CONSTRAINT [FK_dw_FACT_SALES_LINE_DATE] FOREIGN KEY ([date_key]) REFERENCES [dw].[DIM_DATE]([date_key]),
    CONSTRAINT [FK_dw_FACT_SALES_LINE_CUSTOMER] FOREIGN KEY ([customer_key]) REFERENCES [dw].[DIM_CUSTOMER]([customer_key]),
    CONSTRAINT [FK_dw_FACT_SALES_LINE_PRODUCT] FOREIGN KEY ([product_key]) REFERENCES [dw].[DIM_PRODUCT]([product_key]),
    CONSTRAINT [FK_dw_FACT_SALES_LINE_GEO] FOREIGN KEY ([geo_key]) REFERENCES [dw].[DIM_GEOGRAPHY]([geo_key]),
    CONSTRAINT [FK_dw_FACT_SALES_LINE_CHANNEL] FOREIGN KEY ([channel_key]) REFERENCES [dw].[DIM_CHANNEL]([channel_key]),
    CONSTRAINT [FK_dw_FACT_SALES_LINE_STATUS] FOREIGN KEY ([order_status_key]) REFERENCES [dw].[DIM_ORDER_STATUS]([order_status_key])
);
END
GO

IF OBJECT_ID(N'[dw].[FACT_ORDER]', N'U') IS NULL
BEGIN
CREATE TABLE [dw].[FACT_ORDER](
    [order_key] BIGINT NOT NULL,
    [order_id] BIGINT NOT NULL,
    [date_key] INT NOT NULL,
    [customer_key] BIGINT NOT NULL,
    [geo_key] BIGINT NOT NULL,
    [channel_key] BIGINT NOT NULL,
    [order_status_key] BIGINT NOT NULL,
    [gross_order_value] DECIMAL(18,2) NOT NULL,
    [discount_total] DECIMAL(18,2) NOT NULL,
    [order_value] DECIMAL(18,2) NOT NULL,
    [basket_quantity] BIGINT NOT NULL,
    [distinct_products] INT NOT NULL,
    [line_count] INT NOT NULL,
    [payment_value] DECIMAL(18,2) NOT NULL,
    [installments] INT NOT NULL,
    CONSTRAINT [PK_dw_FACT_ORDER] PRIMARY KEY CLUSTERED ([order_key]),
    CONSTRAINT [UQ_dw_FACT_ORDER_order_id] UNIQUE ([order_id]),
    CONSTRAINT [FK_dw_FACT_ORDER_DATE] FOREIGN KEY ([date_key]) REFERENCES [dw].[DIM_DATE]([date_key]),
    CONSTRAINT [FK_dw_FACT_ORDER_CUSTOMER] FOREIGN KEY ([customer_key]) REFERENCES [dw].[DIM_CUSTOMER]([customer_key]),
    CONSTRAINT [FK_dw_FACT_ORDER_GEO] FOREIGN KEY ([geo_key]) REFERENCES [dw].[DIM_GEOGRAPHY]([geo_key]),
    CONSTRAINT [FK_dw_FACT_ORDER_CHANNEL] FOREIGN KEY ([channel_key]) REFERENCES [dw].[DIM_CHANNEL]([channel_key]),
    CONSTRAINT [FK_dw_FACT_ORDER_STATUS] FOREIGN KEY ([order_status_key]) REFERENCES [dw].[DIM_ORDER_STATUS]([order_status_key])
);
END
GO

IF OBJECT_ID(N'[dw].[FACT_CUSTOMER_MONTHLY_SNAPSHOT]', N'U') IS NULL
BEGIN
CREATE TABLE [dw].[FACT_CUSTOMER_MONTHLY_SNAPSHOT](
    [snapshot_month_key] INT NOT NULL,
    [customer_key] BIGINT NOT NULL,
    [monthly_order_count] INT NOT NULL,
    [monthly_quantity] BIGINT NOT NULL,
    [monthly_net_sales] DECIMAL(18,2) NOT NULL,
    [monthly_aov] DECIMAL(18,2) NOT NULL,
    [monthly_basket_size] DECIMAL(18,4) NOT NULL,
    [cumulative_order_count] BIGINT NOT NULL,
    [cumulative_quantity] BIGINT NOT NULL,
    [cumulative_net_sales] DECIMAL(18,2) NOT NULL,
    [cumulative_aov] DECIMAL(18,2) NOT NULL,
    [cumulative_basket_size] DECIMAL(18,4) NOT NULL,
    CONSTRAINT [PK_dw_FACT_CUSTOMER_MONTHLY_SNAPSHOT]
        PRIMARY KEY CLUSTERED ([snapshot_month_key],[customer_key]),
    CONSTRAINT [FK_dw_MONTHLY_DATE] FOREIGN KEY ([snapshot_month_key]) REFERENCES [dw].[DIM_DATE]([date_key]),
    CONSTRAINT [FK_dw_MONTHLY_CUSTOMER] FOREIGN KEY ([customer_key]) REFERENCES [dw].[DIM_CUSTOMER]([customer_key])
);
END
GO

IF OBJECT_ID(N'[dw].[BRIDGE_SALES_LINE_PROMOTION]', N'U') IS NULL
BEGIN
CREATE TABLE [dw].[BRIDGE_SALES_LINE_PROMOTION](
    [sales_line_key] BIGINT NOT NULL,
    [promotion_key] BIGINT NOT NULL,
    [promotion_order] SMALLINT NOT NULL,
    [promotion_count_on_line] SMALLINT NOT NULL,
    [allocation_weight] DECIMAL(12,8) NOT NULL,
    CONSTRAINT [PK_dw_BRIDGE_SALES_LINE_PROMOTION]
        PRIMARY KEY CLUSTERED ([sales_line_key],[promotion_key]),
    CONSTRAINT [CK_dw_BRIDGE_allocation] CHECK ([allocation_weight] > 0 AND [allocation_weight] <= 1),
    CONSTRAINT [FK_dw_BRIDGE_LINE] FOREIGN KEY ([sales_line_key]) REFERENCES [dw].[FACT_SALES_LINE]([sales_line_key]),
    CONSTRAINT [FK_dw_BRIDGE_PROMO] FOREIGN KEY ([promotion_key]) REFERENCES [dw].[DIM_PROMOTION]([promotion_key])
);
END
GO

/* DW analytical indexes */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_dw_FACT_ORDER_customer_date' AND object_id=OBJECT_ID(N'[dw].[FACT_ORDER]'))
    CREATE NONCLUSTERED INDEX [IX_dw_FACT_ORDER_customer_date] ON [dw].[FACT_ORDER]([customer_key],[date_key]) INCLUDE([order_value],[basket_quantity],[order_status_key]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_dw_FACT_ORDER_status_date' AND object_id=OBJECT_ID(N'[dw].[FACT_ORDER]'))
    CREATE NONCLUSTERED INDEX [IX_dw_FACT_ORDER_status_date] ON [dw].[FACT_ORDER]([order_status_key],[date_key]) INCLUDE([order_value]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_dw_FACT_SALES_LINE_customer_product_date' AND object_id=OBJECT_ID(N'[dw].[FACT_SALES_LINE]'))
    CREATE NONCLUSTERED INDEX [IX_dw_FACT_SALES_LINE_customer_product_date] ON [dw].[FACT_SALES_LINE]([customer_key],[product_key],[date_key]) INCLUDE([quantity],[net_sales],[order_id]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_dw_FACT_SALES_LINE_order' AND object_id=OBJECT_ID(N'[dw].[FACT_SALES_LINE]'))
    CREATE NONCLUSTERED INDEX [IX_dw_FACT_SALES_LINE_order] ON [dw].[FACT_SALES_LINE]([order_id]) INCLUDE([net_sales],[quantity]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_dw_MONTHLY_customer' AND object_id=OBJECT_ID(N'[dw].[FACT_CUSTOMER_MONTHLY_SNAPSHOT]'))
    CREATE NONCLUSTERED INDEX [IX_dw_MONTHLY_customer] ON [dw].[FACT_CUSTOMER_MONTHLY_SNAPSHOT]([customer_key],[snapshot_month_key]);
GO

/* ============================================================================
   C. GOLD V2 / CUSTOMER ANALYTICS DATA MART
   ============================================================================ */

IF OBJECT_ID(N'[gold].[FACT_CUSTOMER_ORDER_ITEM]', N'U') IS NULL
BEGIN
CREATE TABLE [gold].[FACT_CUSTOMER_ORDER_ITEM](
    [order_item_id] BIGINT NOT NULL,
    [order_id] BIGINT NOT NULL,
    [customer_id] BIGINT NOT NULL,
    [order_date] DATE NOT NULL,
    [year] INT NOT NULL,
    [month] INT NOT NULL,
    [year_month] NVARCHAR(7) NOT NULL,
    [day_of_week] NVARCHAR(10) NOT NULL,
    [product_id] BIGINT NOT NULL,
    [category] NVARCHAR(100) NOT NULL,
    [quantity] INT NOT NULL,
    [unit_price] DECIMAL(18,2) NOT NULL,
    [discount_amount] DECIMAL(18,2) NOT NULL,
    [line_gross_sales] DECIMAL(18,2) NOT NULL,
    [line_net_sales] DECIMAL(18,2) NOT NULL,
    CONSTRAINT [PK_gold_FACT_CUSTOMER_ORDER_ITEM] PRIMARY KEY CLUSTERED ([order_item_id]),
    CONSTRAINT [FK_gold_ITEM_core_ITEM] FOREIGN KEY ([order_item_id]) REFERENCES [core].[ORDER_ITEMS]([order_item_id]),
    CONSTRAINT [FK_gold_ITEM_core_ORDER] FOREIGN KEY ([order_id]) REFERENCES [core].[ORDERS]([order_id]),
    CONSTRAINT [FK_gold_ITEM_core_CUSTOMER] FOREIGN KEY ([customer_id]) REFERENCES [core].[CUSTOMERS]([customer_id]),
    CONSTRAINT [FK_gold_ITEM_core_PRODUCT] FOREIGN KEY ([product_id]) REFERENCES [core].[PRODUCTS]([product_id])
);
END
GO

IF OBJECT_ID(N'[gold].[FACT_CUSTOMER_ORDER]', N'U') IS NULL
BEGIN
CREATE TABLE [gold].[FACT_CUSTOMER_ORDER](
    [order_id] BIGINT NOT NULL,
    [customer_id] BIGINT NOT NULL,
    [order_date] DATE NOT NULL,
    [year_month] NVARCHAR(7) NOT NULL,
    [day_of_week] NVARCHAR(10) NOT NULL,
    [order_status] NVARCHAR(30) NOT NULL,
    [order_value] DECIMAL(18,2) NOT NULL,
    [basket_quantity] BIGINT NOT NULL,
    [distinct_products] INT NOT NULL,
    [line_count] INT NOT NULL,
    CONSTRAINT [PK_gold_FACT_CUSTOMER_ORDER] PRIMARY KEY CLUSTERED ([order_id]),
    CONSTRAINT [FK_gold_ORDER_core_ORDER] FOREIGN KEY ([order_id]) REFERENCES [core].[ORDERS]([order_id]),
    CONSTRAINT [FK_gold_ORDER_core_CUSTOMER] FOREIGN KEY ([customer_id]) REFERENCES [core].[CUSTOMERS]([customer_id])
);
END
GO

IF OBJECT_ID(N'[gold].[MART_CUSTOMER_360]', N'U') IS NULL
BEGIN
CREATE TABLE [gold].[MART_CUSTOMER_360](
    [customer_id] BIGINT NOT NULL,
    [signup_date] DATE NOT NULL,
    [gender] NVARCHAR(20) NOT NULL,
    [age_group] NVARCHAR(20) NOT NULL,
    [acquisition_channel] NVARCHAR(50) NOT NULL,
    [zip] NVARCHAR(20) NOT NULL,
    [city] NVARCHAR(100) NOT NULL,
    [district] NVARCHAR(100) NOT NULL,
    [region] NVARCHAR(50) NOT NULL,
    [first_order_date] DATE NULL,
    [last_order_date] DATE NULL,
    [order_count] BIGINT NOT NULL,
    [total_quantity] BIGINT NOT NULL,
    [total_net_sales] DECIMAL(18,2) NOT NULL,
    [avg_order_value] DECIMAL(18,2) NULL,
    [avg_basket_size] DECIMAL(18,4) NULL,
    [avg_days_between_orders] DECIMAL(18,2) NULL,
    [customer_lifespan_days] INT NULL,
    [frequency] BIGINT NOT NULL,
    [monetary] DECIMAL(18,2) NOT NULL,
    [frequency_level] NVARCHAR(10) NULL,
    [monetary_level] NVARCHAR(10) NULL,
    [customer_type] NVARCHAR(20) NOT NULL,
    [customer_segment] NVARCHAR(20) NULL,
    [historical_clv] DECIMAL(18,2) NOT NULL,
    [preferred_category] NVARCHAR(100) NULL,
    [preferred_category_share] DECIMAL(8,4) NULL,
    CONSTRAINT [PK_gold_MART_CUSTOMER_360] PRIMARY KEY CLUSTERED ([customer_id]),
    CONSTRAINT [FK_gold_360_core_CUSTOMER] FOREIGN KEY ([customer_id]) REFERENCES [core].[CUSTOMERS]([customer_id])
);
END
GO

IF OBJECT_ID(N'[gold].[MART_CUSTOMER_MONTHLY]', N'U') IS NULL
BEGIN
CREATE TABLE [gold].[MART_CUSTOMER_MONTHLY](
    [customer_id] BIGINT NOT NULL,
    [year_month] NVARCHAR(7) NOT NULL,
    [orders] INT NOT NULL,
    [units] BIGINT NOT NULL,
    [net_sales] DECIMAL(18,2) NOT NULL,
    [avg_order_value] DECIMAL(18,2) NOT NULL,
    [avg_basket_size] DECIMAL(18,4) NOT NULL,
    [customer_type] NVARCHAR(20) NOT NULL,
    CONSTRAINT [PK_gold_MART_CUSTOMER_MONTHLY] PRIMARY KEY CLUSTERED ([customer_id],[year_month]),
    CONSTRAINT [FK_gold_MONTHLY_core_CUSTOMER] FOREIGN KEY ([customer_id]) REFERENCES [core].[CUSTOMERS]([customer_id])
);
END
GO

IF OBJECT_ID(N'[gold].[MART_CUSTOMER_CATEGORY]', N'U') IS NULL
BEGIN
CREATE TABLE [gold].[MART_CUSTOMER_CATEGORY](
    [customer_id] BIGINT NOT NULL,
    [category] NVARCHAR(100) NOT NULL,
    [orders] INT NOT NULL,
    [units] BIGINT NOT NULL,
    [net_sales] DECIMAL(18,2) NOT NULL,
    [revenue_share] DECIMAL(8,4) NOT NULL,
    [category_rank] INT NOT NULL,
    [is_preferred_category] BIT NOT NULL,
    CONSTRAINT [PK_gold_MART_CUSTOMER_CATEGORY] PRIMARY KEY CLUSTERED ([customer_id],[category]),
    CONSTRAINT [FK_gold_CATEGORY_core_CUSTOMER] FOREIGN KEY ([customer_id]) REFERENCES [core].[CUSTOMERS]([customer_id])
);
END
GO

IF OBJECT_ID(N'[gold].[MART_CUSTOMER_TIME]', N'U') IS NULL
BEGIN
CREATE TABLE [gold].[MART_CUSTOMER_TIME](
    [time_grain] NVARCHAR(20) NOT NULL,
    [period_key] NVARCHAR(20) NOT NULL,
    [period_label] NVARCHAR(50) NOT NULL,
    [orders] INT NOT NULL,
    [customers] INT NOT NULL,
    [units] BIGINT NOT NULL,
    [net_sales] DECIMAL(18,2) NOT NULL,
    [avg_order_value] DECIMAL(18,2) NOT NULL,
    [avg_basket_size] DECIMAL(18,4) NOT NULL,
    [repeat_customers] INT NOT NULL,
    [one_time_customers] INT NOT NULL,
    CONSTRAINT [PK_gold_MART_CUSTOMER_TIME] PRIMARY KEY CLUSTERED ([time_grain],[period_key])
);
END
GO

IF OBJECT_ID(N'[gold].[MART_CUSTOMER_SEGMENT_SUMMARY]', N'U') IS NULL
BEGIN
CREATE TABLE [gold].[MART_CUSTOMER_SEGMENT_SUMMARY](
    [customer_segment] NVARCHAR(20) NOT NULL,
    [customer_count] INT NOT NULL,
    [customer_share] DECIMAL(8,4) NOT NULL,
    [orders] BIGINT NOT NULL,
    [units] BIGINT NOT NULL,
    [net_sales] DECIMAL(18,2) NOT NULL,
    [revenue_share] DECIMAL(8,4) NOT NULL,
    [avg_frequency] DECIMAL(12,2) NOT NULL,
    [avg_monetary] DECIMAL(18,2) NOT NULL,
    [avg_aov] DECIMAL(18,2) NOT NULL,
    [avg_basket_size] DECIMAL(18,4) NOT NULL,
    [avg_historical_clv] DECIMAL(18,2) NOT NULL,
    CONSTRAINT [PK_gold_MART_CUSTOMER_SEGMENT_SUMMARY] PRIMARY KEY CLUSTERED ([customer_segment])
);
END
GO

/* Gold indexes for EDA / Power BI */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_gold_ORDER_customer_date' AND object_id=OBJECT_ID(N'[gold].[FACT_CUSTOMER_ORDER]'))
    CREATE NONCLUSTERED INDEX [IX_gold_ORDER_customer_date] ON [gold].[FACT_CUSTOMER_ORDER]([customer_id],[order_date]) INCLUDE([order_status],[order_value],[basket_quantity]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_gold_ORDER_status_date' AND object_id=OBJECT_ID(N'[gold].[FACT_CUSTOMER_ORDER]'))
    CREATE NONCLUSTERED INDEX [IX_gold_ORDER_status_date] ON [gold].[FACT_CUSTOMER_ORDER]([order_status],[order_date]) INCLUDE([customer_id],[order_value]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_gold_ITEM_customer_category' AND object_id=OBJECT_ID(N'[gold].[FACT_CUSTOMER_ORDER_ITEM]'))
    CREATE NONCLUSTERED INDEX [IX_gold_ITEM_customer_category] ON [gold].[FACT_CUSTOMER_ORDER_ITEM]([customer_id],[category]) INCLUDE([order_id],[quantity],[line_net_sales]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_gold_360_segment_type' AND object_id=OBJECT_ID(N'[gold].[MART_CUSTOMER_360]'))
    CREATE NONCLUSTERED INDEX [IX_gold_360_segment_type] ON [gold].[MART_CUSTOMER_360]([customer_segment],[customer_type]) INCLUDE([order_count],[total_net_sales],[historical_clv]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_gold_MONTHLY_year_month' AND object_id=OBJECT_ID(N'[gold].[MART_CUSTOMER_MONTHLY]'))
    CREATE NONCLUSTERED INDEX [IX_gold_MONTHLY_year_month] ON [gold].[MART_CUSTOMER_MONTHLY]([year_month]) INCLUDE([orders],[units],[net_sales],[customer_type]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_gold_CATEGORY_category' AND object_id=OBJECT_ID(N'[gold].[MART_CUSTOMER_CATEGORY]'))
    CREATE NONCLUSTERED INDEX [IX_gold_CATEGORY_category] ON [gold].[MART_CUSTOMER_CATEGORY]([category]) INCLUDE([orders],[units],[net_sales],[is_preferred_category]);
GO

PRINT N'DWH_Problem3 schema creation completed.';
GO
