/*
===============================================================================
03_validate_DWH_Problem3_SQLServer2012.sql
Target: SQL Server 2012 / DWH_Problem3

Purpose
-------
1. Validate expected row counts after Python load.
2. Reconcile revenue across core -> DW -> Gold V2.
3. Validate official EDA V2 order-status scope:
      delivered + paid + shipped
4. Surface known source-quality REVIEW items without silently correcting them.

Important
---------
Gold V2 tables loaded here preserve ALL statuses because gold_problem3_v2.zip was
built for reconciliation. Official EDA V2 recomputes metrics using the scope
(delivered, paid, shipped).
===============================================================================
*/

USE [DWH_Problem3];
GO
SET NOCOUNT ON;
GO

IF OBJECT_ID('tempdb..#ValidationResults') IS NOT NULL
    DROP TABLE #ValidationResults;

CREATE TABLE #ValidationResults(
    [check_id] NVARCHAR(50) NOT NULL,
    [category] NVARCHAR(30) NOT NULL,
    [description] NVARCHAR(255) NOT NULL,
    [actual_value] NVARCHAR(100) NULL,
    [expected_value] NVARCHAR(100) NULL,
    [status] NVARCHAR(10) NOT NULL,
    [note] NVARCHAR(500) NULL
);

/* ============================================================================
   A. ROW COUNTS
   ============================================================================ */

DECLARE @n BIGINT;

/* Core 3NF */
SELECT @n=COUNT_BIG(*) FROM [core].[GEOGRAPHY];
INSERT #ValidationResults VALUES (N'ROW-C01',N'ROW_COUNT',N'core.GEOGRAPHY',
 CONVERT(NVARCHAR(100),@n),N'39948',CASE WHEN @n=39948 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [core].[CUSTOMERS];
INSERT #ValidationResults VALUES (N'ROW-C02',N'ROW_COUNT',N'core.CUSTOMERS',
 CONVERT(NVARCHAR(100),@n),N'121930',CASE WHEN @n=121930 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [core].[SALES_EMPLOYEES];
INSERT #ValidationResults VALUES (N'ROW-C03',N'ROW_COUNT',N'core.SALES_EMPLOYEES',
 CONVERT(NVARCHAR(100),@n),N'200',CASE WHEN @n=200 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [core].[ORDERS];
INSERT #ValidationResults VALUES (N'ROW-C04',N'ROW_COUNT',N'core.ORDERS',
 CONVERT(NVARCHAR(100),@n),N'646945',CASE WHEN @n=646945 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [core].[PRODUCTS];
INSERT #ValidationResults VALUES (N'ROW-C05',N'ROW_COUNT',N'core.PRODUCTS',
 CONVERT(NVARCHAR(100),@n),N'2412',CASE WHEN @n=2412 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [core].[ORDER_ITEMS];
INSERT #ValidationResults VALUES (N'ROW-C06',N'ROW_COUNT',N'core.ORDER_ITEMS',
 CONVERT(NVARCHAR(100),@n),N'714669',CASE WHEN @n=714669 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [core].[PROMOTIONS];
INSERT #ValidationResults VALUES (N'ROW-C07',N'ROW_COUNT',N'core.PROMOTIONS',
 CONVERT(NVARCHAR(100),@n),N'50',CASE WHEN @n=50 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [core].[ORDER_ITEM_PROMOTION];
INSERT #ValidationResults VALUES (N'ROW-C08',N'ROW_COUNT',N'core.ORDER_ITEM_PROMOTION',
 CONVERT(NVARCHAR(100),@n),N'276522',CASE WHEN @n=276522 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [core].[PAYMENTS];
INSERT #ValidationResults VALUES (N'ROW-C09',N'ROW_COUNT',N'core.PAYMENTS',
 CONVERT(NVARCHAR(100),@n),N'646945',CASE WHEN @n=646945 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [core].[SHIPMENTS];
INSERT #ValidationResults VALUES (N'ROW-C10',N'ROW_COUNT',N'core.SHIPMENTS',
 CONVERT(NVARCHAR(100),@n),N'566067',CASE WHEN @n=566067 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [core].[SHIPPERS];
INSERT #ValidationResults VALUES (N'ROW-C11',N'ROW_COUNT',N'core.SHIPPERS',
 CONVERT(NVARCHAR(100),@n),N'80',CASE WHEN @n=80 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [core].[REVIEWS];
INSERT #ValidationResults VALUES (N'ROW-C12',N'ROW_COUNT',N'core.REVIEWS',
 CONVERT(NVARCHAR(100),@n),N'113551',CASE WHEN @n=113551 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [core].[RETURNS];
INSERT #ValidationResults VALUES (N'ROW-C13',N'ROW_COUNT',N'core.RETURNS',
 CONVERT(NVARCHAR(100),@n),N'39939',CASE WHEN @n=39939 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [core].[INVENTORY_SNAPSHOT];
INSERT #ValidationResults VALUES (N'ROW-C14',N'ROW_COUNT',N'core.INVENTORY_SNAPSHOT',
 CONVERT(NVARCHAR(100),@n),N'60247',CASE WHEN @n=60247 THEN N'PASS' ELSE N'FAIL' END,NULL);

/* DW */
SELECT @n=COUNT_BIG(*) FROM [dw].[DIM_DATE];
INSERT #ValidationResults VALUES (N'ROW-D01',N'ROW_COUNT',N'dw.DIM_DATE',
 CONVERT(NVARCHAR(100),@n),N'3833',CASE WHEN @n=3833 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [dw].[DIM_GEOGRAPHY];
INSERT #ValidationResults VALUES (N'ROW-D02',N'ROW_COUNT',N'dw.DIM_GEOGRAPHY',
 CONVERT(NVARCHAR(100),@n),N'39948',CASE WHEN @n=39948 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [dw].[DIM_CUSTOMER];
INSERT #ValidationResults VALUES (N'ROW-D03',N'ROW_COUNT',N'dw.DIM_CUSTOMER',
 CONVERT(NVARCHAR(100),@n),N'121930',CASE WHEN @n=121930 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [dw].[DIM_PRODUCT];
INSERT #ValidationResults VALUES (N'ROW-D04',N'ROW_COUNT',N'dw.DIM_PRODUCT',
 CONVERT(NVARCHAR(100),@n),N'2412',CASE WHEN @n=2412 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [dw].[DIM_PROMOTION];
INSERT #ValidationResults VALUES (N'ROW-D05',N'ROW_COUNT',N'dw.DIM_PROMOTION',
 CONVERT(NVARCHAR(100),@n),N'50',CASE WHEN @n=50 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [dw].[DIM_CHANNEL];
INSERT #ValidationResults VALUES (N'ROW-D06',N'ROW_COUNT',N'dw.DIM_CHANNEL',
 CONVERT(NVARCHAR(100),@n),N'90',CASE WHEN @n=90 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [dw].[DIM_ORDER_STATUS];
INSERT #ValidationResults VALUES (N'ROW-D07',N'ROW_COUNT',N'dw.DIM_ORDER_STATUS',
 CONVERT(NVARCHAR(100),@n),N'6',CASE WHEN @n=6 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [dw].[FACT_SALES_LINE];
INSERT #ValidationResults VALUES (N'ROW-D08',N'ROW_COUNT',N'dw.FACT_SALES_LINE',
 CONVERT(NVARCHAR(100),@n),N'714669',CASE WHEN @n=714669 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [dw].[FACT_ORDER];
INSERT #ValidationResults VALUES (N'ROW-D09',N'ROW_COUNT',N'dw.FACT_ORDER',
 CONVERT(NVARCHAR(100),@n),N'646945',CASE WHEN @n=646945 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [dw].[FACT_CUSTOMER_MONTHLY_SNAPSHOT];
INSERT #ValidationResults VALUES (N'ROW-D10',N'ROW_COUNT',N'dw.FACT_CUSTOMER_MONTHLY_SNAPSHOT',
 CONVERT(NVARCHAR(100),@n),N'586976',CASE WHEN @n=586976 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [dw].[BRIDGE_SALES_LINE_PROMOTION];
INSERT #ValidationResults VALUES (N'ROW-D11',N'ROW_COUNT',N'dw.BRIDGE_SALES_LINE_PROMOTION',
 CONVERT(NVARCHAR(100),@n),N'276522',CASE WHEN @n=276522 THEN N'PASS' ELSE N'FAIL' END,NULL);

/* Gold V2 */
SELECT @n=COUNT_BIG(*) FROM [gold].[FACT_CUSTOMER_ORDER_ITEM];
INSERT #ValidationResults VALUES (N'ROW-G01',N'ROW_COUNT',N'gold.FACT_CUSTOMER_ORDER_ITEM',
 CONVERT(NVARCHAR(100),@n),N'714669',CASE WHEN @n=714669 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [gold].[FACT_CUSTOMER_ORDER];
INSERT #ValidationResults VALUES (N'ROW-G02',N'ROW_COUNT',N'gold.FACT_CUSTOMER_ORDER',
 CONVERT(NVARCHAR(100),@n),N'646945',CASE WHEN @n=646945 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [gold].[MART_CUSTOMER_360];
INSERT #ValidationResults VALUES (N'ROW-G03',N'ROW_COUNT',N'gold.MART_CUSTOMER_360',
 CONVERT(NVARCHAR(100),@n),N'121930',CASE WHEN @n=121930 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [gold].[MART_CUSTOMER_MONTHLY];
INSERT #ValidationResults VALUES (N'ROW-G04',N'ROW_COUNT',N'gold.MART_CUSTOMER_MONTHLY',
 CONVERT(NVARCHAR(100),@n),N'586976',CASE WHEN @n=586976 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [gold].[MART_CUSTOMER_CATEGORY];
INSERT #ValidationResults VALUES (N'ROW-G05',N'ROW_COUNT',N'gold.MART_CUSTOMER_CATEGORY',
 CONVERT(NVARCHAR(100),@n),N'176822',CASE WHEN @n=176822 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [gold].[MART_CUSTOMER_TIME];
INSERT #ValidationResults VALUES (N'ROW-G06',N'ROW_COUNT',N'gold.MART_CUSTOMER_TIME',
 CONVERT(NVARCHAR(100),@n),N'133',CASE WHEN @n=133 THEN N'PASS' ELSE N'FAIL' END,NULL);

SELECT @n=COUNT_BIG(*) FROM [gold].[MART_CUSTOMER_SEGMENT_SUMMARY];
INSERT #ValidationResults VALUES (N'ROW-G07',N'ROW_COUNT',N'gold.MART_CUSTOMER_SEGMENT_SUMMARY',
 CONVERT(NVARCHAR(100),@n),N'4',CASE WHEN @n=4 THEN N'PASS' ELSE N'FAIL' END,NULL);

/* ============================================================================
   B. REVENUE RECONCILIATION - ALL STATUS GOLD V2
   ============================================================================ */

DECLARE
    @core_item_net DECIMAL(38,2),
    @core_payment DECIMAL(38,2),
    @dw_line_net DECIMAL(38,2),
    @dw_order_value DECIMAL(38,2),
    @gold_line_net DECIMAL(38,2),
    @gold_order_value DECIMAL(38,2),
    @gold_customer_total DECIMAL(38,2),
    @gold_month_total DECIMAL(38,2),
    @expected_all DECIMAL(38,2);

SET @expected_all = CAST(15680869265.43 AS DECIMAL(38,2));

SELECT @core_item_net =
    SUM(CAST([quantity] AS DECIMAL(38,2))*CAST([unit_price] AS DECIMAL(38,2))
        - CAST([discount_amount] AS DECIMAL(38,2)))
FROM [core].[ORDER_ITEMS];

SELECT @core_payment = SUM(CAST([payment_value] AS DECIMAL(38,2)))
FROM [core].[PAYMENTS];

SELECT @dw_line_net = SUM(CAST([net_sales] AS DECIMAL(38,2)))
FROM [dw].[FACT_SALES_LINE];

SELECT @dw_order_value = SUM(CAST([order_value] AS DECIMAL(38,2)))
FROM [dw].[FACT_ORDER];

SELECT @gold_line_net = SUM(CAST([line_net_sales] AS DECIMAL(38,2)))
FROM [gold].[FACT_CUSTOMER_ORDER_ITEM];

SELECT @gold_order_value = SUM(CAST([order_value] AS DECIMAL(38,2)))
FROM [gold].[FACT_CUSTOMER_ORDER];

SELECT @gold_customer_total = SUM(CAST([total_net_sales] AS DECIMAL(38,2)))
FROM [gold].[MART_CUSTOMER_360];

SELECT @gold_month_total = SUM(CAST([net_sales] AS DECIMAL(38,2)))
FROM [gold].[MART_CUSTOMER_MONTHLY];

INSERT #ValidationResults VALUES
(N'REV-01',N'RECONCILIATION',N'Core ORDER_ITEMS Net Sales',
 CONVERT(NVARCHAR(100),@core_item_net),CONVERT(NVARCHAR(100),@expected_all),
 CASE WHEN ABS(@core_item_net-@expected_all)<=0.01 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'REV-02',N'RECONCILIATION',N'Core PAYMENTS payment_value',
 CONVERT(NVARCHAR(100),@core_payment),CONVERT(NVARCHAR(100),@expected_all),
 CASE WHEN ABS(@core_payment-@expected_all)<=0.01 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'REV-03',N'RECONCILIATION',N'DW FACT_SALES_LINE Net Sales',
 CONVERT(NVARCHAR(100),@dw_line_net),CONVERT(NVARCHAR(100),@expected_all),
 CASE WHEN ABS(@dw_line_net-@expected_all)<=0.01 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'REV-04',N'RECONCILIATION',N'DW FACT_ORDER Order Value',
 CONVERT(NVARCHAR(100),@dw_order_value),CONVERT(NVARCHAR(100),@expected_all),
 CASE WHEN ABS(@dw_order_value-@expected_all)<=0.01 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'REV-05',N'RECONCILIATION',N'Gold FACT_CUSTOMER_ORDER_ITEM Net Sales',
 CONVERT(NVARCHAR(100),@gold_line_net),CONVERT(NVARCHAR(100),@expected_all),
 CASE WHEN ABS(@gold_line_net-@expected_all)<=0.01 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'REV-06',N'RECONCILIATION',N'Gold FACT_CUSTOMER_ORDER Order Value',
 CONVERT(NVARCHAR(100),@gold_order_value),CONVERT(NVARCHAR(100),@expected_all),
 CASE WHEN ABS(@gold_order_value-@expected_all)<=0.01 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'REV-07',N'RECONCILIATION',N'Gold MART_CUSTOMER_360 total',
 CONVERT(NVARCHAR(100),@gold_customer_total),CONVERT(NVARCHAR(100),@expected_all),
 CASE WHEN ABS(@gold_customer_total-@expected_all)<=0.01 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'REV-08',N'RECONCILIATION',N'Gold MART_CUSTOMER_MONTHLY total',
 CONVERT(NVARCHAR(100),@gold_month_total),CONVERT(NVARCHAR(100),@expected_all),
 CASE WHEN ABS(@gold_month_total-@expected_all)<=0.01 THEN N'PASS' ELSE N'FAIL' END,NULL);

/* ============================================================================
   C. GOLD V2 CUSTOMER RULE CHECKS - ALL STATUS PACKAGE
   ============================================================================ */

DECLARE
    @buyers BIGINT,
    @repeat BIGINT,
    @one_time BIGINT,
    @no_purchase BIGINT,
    @segment_buyers BIGINT;

SELECT
    @buyers = SUM(CASE WHEN [order_count] > 0 THEN 1 ELSE 0 END),
    @repeat = SUM(CASE WHEN [customer_type] = N'Repeat' THEN 1 ELSE 0 END),
    @one_time = SUM(CASE WHEN [customer_type] = N'One-time' THEN 1 ELSE 0 END),
    @no_purchase = SUM(CASE WHEN [customer_type] = N'No-purchase' THEN 1 ELSE 0 END),
    @segment_buyers = SUM(CASE WHEN [customer_segment] IS NOT NULL THEN 1 ELSE 0 END)
FROM [gold].[MART_CUSTOMER_360];

INSERT #ValidationResults VALUES
(N'CUS-01',N'BUSINESS_RULE',N'Gold buyers',
 CONVERT(NVARCHAR(100),@buyers),N'90246',
 CASE WHEN @buyers=90246 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'CUS-02',N'BUSINESS_RULE',N'Gold Repeat customers',
 CONVERT(NVARCHAR(100),@repeat),N'67888',
 CASE WHEN @repeat=67888 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'CUS-03',N'BUSINESS_RULE',N'Gold One-time customers',
 CONVERT(NVARCHAR(100),@one_time),N'22358',
 CASE WHEN @one_time=22358 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'CUS-04',N'BUSINESS_RULE',N'Gold No-purchase customers',
 CONVERT(NVARCHAR(100),@no_purchase),N'31684',
 CASE WHEN @no_purchase=31684 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'CUS-05',N'BUSINESS_RULE',N'Every all-status buyer has one F x M segment',
 CONVERT(NVARCHAR(100),@segment_buyers),CONVERT(NVARCHAR(100),@buyers),
 CASE WHEN @segment_buyers=@buyers THEN N'PASS' ELSE N'FAIL' END,NULL);

/* ============================================================================
   D. OFFICIAL EDA V2 STATUS SCOPE
      INCLUDED: delivered, paid, shipped
      EXCLUDED: created, cancelled, returned
   ============================================================================ */

DECLARE
    @official_orders BIGINT,
    @official_sales DECIMAL(38,2),
    @official_line_sales DECIMAL(38,2),
    @official_buyers BIGINT,
    @official_repeat BIGINT,
    @official_one_time BIGINT,
    @official_no_purchase BIGINT;

SELECT
    @official_orders = COUNT_BIG(*),
    @official_sales = SUM(CAST([order_value] AS DECIMAL(38,2)))
FROM [gold].[FACT_CUSTOMER_ORDER]
WHERE [order_status] IN (N'delivered',N'paid',N'shipped');

SELECT @official_line_sales = SUM(CAST(i.[line_net_sales] AS DECIMAL(38,2)))
FROM [gold].[FACT_CUSTOMER_ORDER_ITEM] i
INNER JOIN [gold].[FACT_CUSTOMER_ORDER] o
    ON o.[order_id]=i.[order_id]
WHERE o.[order_status] IN (N'delivered',N'paid',N'shipped');

SELECT
    @official_buyers = COUNT_BIG(*),
    @official_repeat = SUM(CASE WHEN x.order_count >= 2 THEN 1 ELSE 0 END),
    @official_one_time = SUM(CASE WHEN x.order_count = 1 THEN 1 ELSE 0 END)
FROM (
    SELECT [customer_id], COUNT_BIG(DISTINCT [order_id]) AS order_count
    FROM [gold].[FACT_CUSTOMER_ORDER]
    WHERE [order_status] IN (N'delivered',N'paid',N'shipped')
    GROUP BY [customer_id]
) x;

SELECT @official_no_purchase = COUNT_BIG(*) - @official_buyers
FROM [core].[CUSTOMERS];

INSERT #ValidationResults VALUES
(N'SCOPE-01',N'OFFICIAL_SCOPE',N'Official EDA V2 order count: delivered + paid + shipped',
 CONVERT(NVARCHAR(100),@official_orders),N'544066',
 CASE WHEN @official_orders=544066 THEN N'PASS' ELSE N'FAIL' END,
 N'Official analytical scope, not the storage scope of Gold V2.');

INSERT #ValidationResults VALUES
(N'SCOPE-02',N'OFFICIAL_SCOPE',N'Official EDA V2 Net Sales',
 CONVERT(NVARCHAR(100),@official_sales),N'13186081893.64',
 CASE WHEN ABS(@official_sales-CAST(13186081893.64 AS DECIMAL(38,2)))<=0.01 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'SCOPE-03',N'OFFICIAL_SCOPE',N'Official item-level Net Sales reconciles to official orders',
 CONVERT(NVARCHAR(100),@official_line_sales),CONVERT(NVARCHAR(100),@official_sales),
 CASE WHEN ABS(@official_line_sales-@official_sales)<=0.01 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'SCOPE-04',N'OFFICIAL_SCOPE',N'Official buyers',
 CONVERT(NVARCHAR(100),@official_buyers),N'86333',
 CASE WHEN @official_buyers=86333 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'SCOPE-05',N'OFFICIAL_SCOPE',N'Official Repeat customers',
 CONVERT(NVARCHAR(100),@official_repeat),N'63532',
 CASE WHEN @official_repeat=63532 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'SCOPE-06',N'OFFICIAL_SCOPE',N'Official One-time customers',
 CONVERT(NVARCHAR(100),@official_one_time),N'22801',
 CASE WHEN @official_one_time=22801 THEN N'PASS' ELSE N'FAIL' END,NULL);

INSERT #ValidationResults VALUES
(N'SCOPE-07',N'OFFICIAL_SCOPE',N'Official No-purchase customers',
 CONVERT(NVARCHAR(100),@official_no_purchase),N'35597',
 CASE WHEN @official_no_purchase=35597 THEN N'PASS' ELSE N'FAIL' END,NULL);

/* ============================================================================
   E. KNOWN SOURCE-QUALITY REVIEW ITEMS
   ============================================================================ */

DECLARE @shipment_before_join BIGINT;

SELECT @shipment_before_join = COUNT_BIG(*)
FROM [core].[SHIPMENTS] s
INNER JOIN [core].[SHIPPERS] p
    ON p.[shipper_id]=s.[shipper_id]
WHERE s.[ship_date] < p.[join_date];

INSERT #ValidationResults VALUES
(N'REVIEW-01',N'DATA_QUALITY',N'Shipments with ship_date before shipper join_date',
 CONVERT(NVARCHAR(100),@shipment_before_join),N'519276',
 CASE WHEN @shipment_before_join=519276 THEN N'REVIEW' ELSE N'REVIEW' END,
 N'Known source inconsistency retained and flagged; do not auto-correct without business semantics.');

/* ============================================================================
   F. RESULTS
   ============================================================================ */

SELECT *
FROM #ValidationResults
ORDER BY
    CASE [status] WHEN N'FAIL' THEN 1 WHEN N'REVIEW' THEN 2 ELSE 3 END,
    [check_id];

SELECT
    [status],
    COUNT(*) AS [check_count]
FROM #ValidationResults
GROUP BY [status]
ORDER BY
    CASE [status] WHEN N'FAIL' THEN 1 WHEN N'REVIEW' THEN 2 ELSE 3 END;

IF EXISTS (SELECT 1 FROM #ValidationResults WHERE [status]=N'FAIL')
    RAISERROR(N'DWH_Problem3 validation completed with FAIL checks.', 16, 1);
ELSE
    PRINT N'DWH_Problem3 validation completed with no FAIL checks.';
GO
