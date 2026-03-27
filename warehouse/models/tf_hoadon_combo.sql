{{ config(materialized='table') }}

WITH hoadon_combo_hanghoa AS (
    SELECT * FROM {{ source('analytics_raw', 'raw_hoadon_combo_hanghoa') }}
),
hanghoa AS (
    SELECT * FROM {{ source('analytics_raw', 'raw_hanghoa') }}
)
SELECT 
    HD_CB_HH.bill_id,
    HD_CB_HH.order_id AS hdcthh_id,
    UPPER(LTRIM(RTRIM(HD_CB_HH.goods_id))) AS goods_id,
    UPPER(LTRIM(RTRIM(HH.name))) AS goods_name,
    HD_CB_HH.quantity,
    HD_CB_HH.price
FROM hoadon_combo_hanghoa HD_CB_HH
JOIN hanghoa HH 
    ON HD_CB_HH.goods_id = HH.code
WHERE 
    HD_CB_HH.trangthai = 4
    AND CAST(HD_CB_HH.created_date AS DATE) >= '2025-01-01'
