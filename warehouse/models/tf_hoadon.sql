{{ config(materialized='table') }}

WITH cuahang AS (
    SELECT * FROM {{ source('analytics_raw', 'raw_cuahang') }}
),
ca AS (
    SELECT * FROM {{ source('analytics_raw', 'raw_ca') }}
),
hoadon AS (
    SELECT * FROM {{ source('analytics_raw', 'raw_transactions') }}
)
SELECT
    CAST(Ca.date AS DATE) AS sale_date,
    UPPER(REPLACE(CuaHang.name, 'ICOOL ', '')) AS branch,
    HoaDon.id AS bill_id,
    HoaDon.room_id AS room, 
    HoaDon.start AS `start`,
    HoaDon.finish AS `finish`,
    ROUND(HoaDon.total, 0) + ROUND(HoaDon.vat, 0) + ROUND(HoaDon.phuphi, 0) AS revenue,
    HoaDon.user_id
FROM cuahang CuaHang
JOIN ca Ca ON CuaHang.code = Ca.cuahang_id
JOIN hoadon HoaDon ON Ca.id = HoaDon.ca_id
    AND Ca.cuahang_id = HoaDon.cuahang_id
WHERE  
    CuaHang.trangthai = 4
    AND Ca.trangthai = 4
    AND HoaDon.trangthai = 4
    AND CAST(Ca.date AS DATE) >= '2025-01-01'
