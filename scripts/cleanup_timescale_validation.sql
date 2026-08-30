-- ============================================================
-- ECDT - TimescaleDB validation cleanup
-- ============================================================
--
-- This script removes ONLY data created during TimescaleDB
-- validation tests.
--
-- It does NOT remove the Phase 2 source files.
-- It does NOT remove the TimescaleDB schema.
-- It does NOT remove the hypertable.
-- ============================================================


-- ------------------------------------------------------------
-- 1. Remove the artificial validation observation
-- ------------------------------------------------------------

DELETE FROM metric_observations
WHERE case_id = 'validation_case_001';


-- ------------------------------------------------------------
-- 2. Remove duplicate validation insertion
-- ------------------------------------------------------------
--
-- Keep ONE copy of an identical observation and remove
-- additional duplicates.
--
-- The query below keeps the first row for each identical
-- observation.
-- ------------------------------------------------------------

WITH duplicates AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY
                resource_id,
                timestamp,
                value,
                metric_type,
                metric_name,
                case_id
            ORDER BY ctid
        ) AS row_number
    FROM metric_observations
    WHERE case_id = 're2ob_checkoutservice_cpu_1'
)
DELETE FROM metric_observations
WHERE ctid IN (
    SELECT ctid
    FROM duplicates
    WHERE row_number > 1
);


-- ------------------------------------------------------------
-- 3. Verification
-- ------------------------------------------------------------

SELECT
    COUNT(*) AS total_observations
FROM metric_observations;


SELECT
    case_id,
    COUNT(*) AS observations
FROM metric_observations
GROUP BY case_id
ORDER BY case_id;