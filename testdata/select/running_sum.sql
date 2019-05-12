-- from https://support.treasuredata.com/hc/ja/articles/216392117-Window-%E9%96%A2%E6%95%B0%E3%82%92%E4%BD%BF%E3%81%84%E3%81%93%E3%81%AA%E3%81%99-%E9%9B%86%E7%B4%84%E9%96%A2%E6%95%B0%E7%B3%BB-
SELECT m, d, goods_id, sales, SUM(sales) OVER (PARTITION BY goods_id,m ORDER BY d ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as sales_running_sum
FROM
(
  SELECT
    TD_TIME_FORMAT(time,'yyyy-MM-dd','JST') AS d, TD_TIME_FORMAT(time,'yyyy-MM','JST') AS m, goods_id, SUM(price*amount) AS sales
  FROM  sales_slip
  GROUP BY TD_TIME_FORMAT(time,'yyyy-MM-dd','JST'), TD_TIME_FORMAT(time,'yyyy-MM','JST'), goods_id
) t
ORDER BY goods_id, m, d