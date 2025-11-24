-- Row count
SELECT COUNT(*) FROM `leapproject2.health_api.covid_cases_api`;

-- Records by state
SELECT state, COUNT(*) AS records
FROM `leapproject2.health_api.covid_cases_api`
GROUP BY state
ORDER BY records DESC;

-- Sample time-based metrics
SELECT _PARTITIONTIME AS partition_date, COUNT(*)
FROM `leapproject2.health_api.covid_cases_api`
GROUP BY partition_date
ORDER BY partition_date DESC;
