# KAN-72 - SQL Server Validation Evidence

## 1. Environment

- Project: DAAI_N1.3 - Customer Atlas
- Database: DWH_Problem3
- Engine: Microsoft SQL Server
- Validation date: 2026-10-23
- Script: sql/reconciliation/03_validate_DWH_Problem3_SQLServer2012.sql

## 2. Results

| Status | Count |
|---|---:|
| PASS | 52 |
| REVIEW | 1 |
| FAIL | 0 |
| TOTAL | 53 |

The SQL validation script executed successfully.
No FAIL checks were reported.

## 3. REVIEW-01

Description:
Shipments with ship_date before shipper join_date.

Observed count: 519276
Status: REVIEW

This issue was retained and flagged.
The underlying business meaning of these dates
requires further confirmation.

The team has not silently modified source dates.

## 4. Revenue Scope

All-status revenue (Core/DW reconciliation):
15,680,869,265.43 VND

Official revenue (OLAP):
13,186,081,893.64 VND

Official scope:
delivered, paid, shipped

Excluded from official scope:
created, cancelled, returned

The two amounts use different analytical scopes
and must not be compared as identical populations.

## 5. Evidence

- KAN-72_SQL_Validation_Detail.csv
- KAN-72_SQL_Validation_Summary.csv
- KAN-72_SQL_Validation.png

## 6. Conclusion

52 PASS, 1 REVIEW, 0 FAIL.

The reported checks provide evidence for the SQL
validation rules tested by the script.

The REVIEW item remains an unresolved source
data-quality observation.

SQL validation does not by itself prove that SSAS
processing or Power BI measures are correct.
Those are validated separately.
