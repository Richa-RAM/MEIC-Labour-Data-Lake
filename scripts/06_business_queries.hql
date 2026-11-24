-- =====================================================================
--  STEP 6 - Business queries
--  Each query answers one question the company used to answer by hand
--  in Excel. Beeline prints the run time under each result
--  ("10 rows selected (12.3 seconds)") - use it for the run-times table.
-- =====================================================================
USE meic_analytics;

-- Q0. Reconciliation: raw vs clean vs rejected record counts
SELECT 'raw (bronze)'     AS zone, COUNT(*) AS records FROM labour_raw
UNION ALL
SELECT 'clean (silver)'   AS zone, COUNT(*) AS records FROM labour_cleaned
UNION ALL
SELECT 'rejected'         AS zone, COUNT(*) AS records FROM labour_rejected;

-- Q1. Which client projects cost the most in labour? (overhead excluded)
SELECT project, total_labour_cost, total_working_hours, distinct_employees,
       cost_per_hour, first_work_date, last_work_date
FROM project_costs
WHERE is_overhead = false
ORDER BY total_labour_cost DESC
LIMIT 10;

-- Q2. How much labour cost is overhead rather than client projects?
SELECT CASE WHEN is_overhead THEN 'Overhead' ELSE 'Client projects' END AS cost_type,
       ROUND(SUM(total_labour_cost), 2) AS labour_cost,
       ROUND(100 * SUM(total_labour_cost) / SUM(SUM(total_labour_cost)) OVER (), 1) AS pct_of_total
FROM project_costs
GROUP BY CASE WHEN is_overhead THEN 'Overhead' ELSE 'Client projects' END;

-- Q3. Who worked the most hours, and on which activity?
SELECT employee, type_of_work, total_working_hours, days_worked, total_labour_cost
FROM employee_work_hours
ORDER BY total_working_hours DESC
LIMIT 10;

-- Q4. Hours and cost by activity (type of work)
SELECT type_of_work,
       ROUND(SUM(total_working_hours), 2) AS hours,
       ROUND(SUM(total_labour_cost), 2)   AS labour_cost
FROM employee_work_hours
GROUP BY type_of_work
ORDER BY hours DESC
LIMIT 15;

-- Q5. Data-quality checks: how many records were rejected or flagged, and why
SELECT check_name, record_count FROM data_quality_summary;

-- Q6. The rejected records themselves
SELECT record_id, work_date, employee, project, total_hours, rate, amount, reject_reason
FROM labour_rejected
ORDER BY total_hours DESC
LIMIT 20;

-- Q7. Employees with the most overtime days (over 12 hours)
SELECT employee, designation, COUNT(*) AS overtime_days, ROUND(MAX(total_hours), 2) AS max_hours
FROM flagged_records
WHERE flag_overtime = true
GROUP BY employee, designation
ORDER BY overtime_days DESC
LIMIT 10;

-- Q8. Labour cost trend by year (2026 is a part year - see months_covered)
SELECT year, total_labour_cost, total_working_hours, distinct_employees,
       months_covered, overhead_cost, cost_per_hour
FROM yearly_trends
ORDER BY year;

-- Q9. Monthly cost in 2024, read straight from silver.
-- WHERE year = 2024 means Hive only reads the 2024 partition folders.
SELECT month, ROUND(SUM(amount), 2) AS labour_cost, ROUND(SUM(total_hours), 2) AS hours
FROM labour_cleaned
WHERE year = 2024
GROUP BY month
ORDER BY month;

-- Q10. Labour cost by source system and year
SELECT year, data_source, ROUND(SUM(amount), 2) AS labour_cost, COUNT(*) AS records
FROM labour_cleaned
GROUP BY year, data_source
ORDER BY year, data_source;
