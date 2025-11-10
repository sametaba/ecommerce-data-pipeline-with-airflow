-- 1. country bazında kaç sipariş var?
CREATE TABLE IF NOT EXISTS fact_country_sales AS
SELECT country,
       COUNT(*) AS order_count
FROM stg_orders
GROUP BY country
ORDER BY order_count DESC;

-- 2. en çok sipariş gelen ilk 10 ülkeyi görmek için
SELECT * FROM fact_country_sales
ORDER BY order_count DESC
LIMIT 10;
