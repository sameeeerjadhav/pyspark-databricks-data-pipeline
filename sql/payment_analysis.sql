-- Payment success rate across all Silver payments.
SELECT
    COUNT(*) AS total_payments,
    SUM(CASE WHEN payment_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_payments,
    SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_payments,
    ROUND(
        SUM(CASE WHEN payment_status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS payment_success_rate
FROM silver_payments;

-- Successful amount by payment method.
SELECT
    payment_method,
    successful_payments,
    failed_payments,
    total_amount
FROM payment_summary
ORDER BY total_amount DESC;
