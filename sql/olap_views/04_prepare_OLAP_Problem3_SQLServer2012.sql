USE [DWH_Problem3]
GO

/****** Object:  View [olap].[V_DIM_DATE]    Script Date: 23/10/2026 9:11:55 CH ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [olap].[V_DIM_DATE]
AS
SELECT
    d.[date_key], d.[full_date], d.[day], d.[day_name], d.[day_of_week],
    d.[week_of_year], d.[month], d.[month_name], d.[quarter], d.[year],
    d.[year_month],
    CAST(d.[year] * 100 + d.[month] AS INT) AS [month_key],
    CAST(CAST(d.[year] AS NVARCHAR(4)) + N'-Q' + CAST(d.[quarter] AS NVARCHAR(1)) AS NVARCHAR(7)) AS [year_quarter],
    d.[is_weekend]
FROM [dw].[DIM_DATE] d;

GO


