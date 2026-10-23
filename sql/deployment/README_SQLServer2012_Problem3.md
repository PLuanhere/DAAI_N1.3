# SQL Server 2012 Deployment Bundle — Problem 3

Bộ này triển khai pipeline hiện tại vào một database SQL Server 2012:

`core (14 bảng 3NF) → dw (Star / Fact Constellation) → gold (Customer Analytics Data Mart)`

## File

1. `01_create_DWH_Problem3_SQLServer2012.sql`  
   Tạo database `DWH_Problem3`, 3 schema `core`, `dw`, `gold`, 32 bảng, PK/FK/constraints/indexes.

2. `02_load_DWH_Problem3_SQLServer2012.py`  
   Đọc trực tiếp 3 ZIP và nạp dữ liệu theo thứ tự FK:
   - `rd3nf_output.zip`
   - `dw_problem3.zip`
   - `gold_problem3_v2.zip`

3. `03_validate_DWH_Problem3_SQLServer2012.sql`  
   Kiểm tra row count, revenue reconciliation, customer rules và Order Status Scope chính thức của EDA V2.

## Cách chạy

### Bước 1 — SSMS

Mở và chạy:

```sql
01_create_DWH_Problem3_SQLServer2012.sql
```

### Bước 2 — Python loader

Cài:

```bash
pip install pyodbc
```

Windows Authentication:

```bat
python 02_load_DWH_Problem3_SQLServer2012.py ^
  --server ".\SQLEXPRESS" ^
  --database DWH_Problem3 ^
  --trusted ^
  --core rd3nf_output.zip ^
  --dw dw_problem3.zip ^
  --gold gold_problem3_v2.zip
```

Nếu máy chỉ có SQL Server Native Client 11.0:

```bat
python 02_load_DWH_Problem3_SQLServer2012.py ^
  --server ".\SQLEXPRESS" ^
  --database DWH_Problem3 ^
  --driver "SQL Server Native Client 11.0" ^
  --trusted ^
  --core rd3nf_output.zip ^
  --dw dw_problem3.zip ^
  --gold gold_problem3_v2.zip
```

Chạy lại từ đầu khi tables đã có dữ liệu:

```bat
... --replace
```

### Bước 3 — Validation

Mở SSMS và chạy:

```sql
03_validate_DWH_Problem3_SQLServer2012.sql
```

Kỳ vọng: không có `FAIL`. `REVIEW-01` là anomaly nguồn đã biết về `ship_date < shipper.join_date`.

## Order Status Scope

Database lưu Gold V2 theo **ALL statuses** để bảo toàn reconciliation.

EDA V2 chính thức dùng:

- included: `delivered`, `paid`, `shipped`
- excluded: `created`, `cancelled`, `returned`

Validation SQL kiểm tra riêng scope này:
- orders = `544,066`
- Net Sales = `13,186,081,893.64`
- buyers = `86,333`
- repeat = `63,532`
- one-time = `22,801`
- no-purchase = `35,597`

Khi xây dashboard SQL/Power BI, cần dùng cùng status rule hoặc xây Data Mart/dashboard layer được recompute theo scope chính thức; không lấy các segment all-status trong Gold V2 rồi chỉ lọc fact ở Power BI.
