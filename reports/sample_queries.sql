-- Sample analytics queries
-- 1) Churn rate
SELECT ROUND(100.0 * AVG(CAST(Churn AS REAL)), 2) AS churn_rate_pct
FROM customers_transformed;

-- 2) Average spend by tenure group
SELECT TenureGroup, ROUND(AVG(AvgMonthlySpend), 2) AS avg_monthly_spend
FROM customers_transformed
GROUP BY TenureGroup
ORDER BY 
  CASE TenureGroup 
    WHEN '0-6' THEN 1
    WHEN '7-12' THEN 2
    WHEN '13-24' THEN 3
    WHEN '25-48' THEN 4
    WHEN '48+' THEN 5
    ELSE 6
  END;

-- 3) Top 10 customers by TotalSpend
SELECT customerID, TotalSpend
FROM customers_transformed
ORDER BY TotalSpend DESC
LIMIT 10;
