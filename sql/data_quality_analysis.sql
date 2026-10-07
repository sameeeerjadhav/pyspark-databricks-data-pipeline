-- Quality summary produced by the latest pipeline run.
SELECT
    dataset,
    total_records,
    valid_records,
    invalid_records,
    duplicate_records,
    null_records,
    quality_percentage
FROM data_quality_report
ORDER BY dataset;

-- Datasets below a 95 percent valid-row threshold.
SELECT
    dataset,
    total_records,
    invalid_records,
    quality_percentage
FROM data_quality_report
WHERE CAST(REPLACE(quality_percentage, '%', '') AS DOUBLE) < 95
ORDER BY CAST(REPLACE(quality_percentage, '%', '') AS DOUBLE);
