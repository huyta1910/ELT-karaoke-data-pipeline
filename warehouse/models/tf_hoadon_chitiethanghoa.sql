{{ config(materialized='table') }}

WITH hoadon_giamgia AS (
    SELECT * FROM {{ source('analytics_raw', 'raw_hoadon_giamgia') }}
),
hoadon_chitiethanghoa AS (
    SELECT * FROM {{ source('analytics_raw', 'raw_hoadon_chitiethanghoa') }}
),
DiscountAgg AS (
    SELECT code2, SUM(total) AS total_discount
    FROM hoadon_giamgia
    WHERE trangthai = 4
    GROUP BY code2
)
SELECT
    HD_CTHH.id AS hdcthh_id,
    HD_CTHH.bill_id,
    UPPER(LTRIM(RTRIM(HD_CTHH.goods_id))) AS goods_id,
    UPPER(LTRIM(RTRIM(HD_CTHH.goods_name))) AS goods_name,
    HD_CTHH.category_id,
    HD_CTHH.price,
    HD_CTHH.time,
    SUM(HD_CTHH.quantity) AS quantity,
    SUM(HD_CTHH.total - COALESCE(DA.total_discount, 0)) AS price_after_discount
FROM hoadon_chitiethanghoa HD_CTHH
LEFT JOIN DiscountAgg DA 
    ON DA.code2 = HD_CTHH.id
WHERE 
    HD_CTHH.trangthai = 4
    AND CAST(HD_CTHH.created_date AS DATE) >= '2025-01-01'
    -- AND HD_CTHH.category_id NOT IN ('TIME', 'OUTSIDE', 'GIFT')
GROUP BY 
    HD_CTHH.id, HD_CTHH.bill_id, HD_CTHH.goods_id, HD_CTHH.goods_name,
    HD_CTHH.category_id, HD_CTHH.price, HD_CTHH.time
