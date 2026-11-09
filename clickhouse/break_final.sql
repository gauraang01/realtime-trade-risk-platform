-- Break it on purpose: one trade, updated out of order and delivered twice.
-- Each insert is a separate part, and nothing merges them until OPTIMIZE at the end.

DROP TABLE IF EXISTS serving.demo_versioned;
DROP TABLE IF EXISTS serving.demo_unversioned;
DROP TABLE IF EXISTS serving.demo_plain;

CREATE TABLE serving.demo_versioned (trade_id String, mtm Int32, kafka_offset UInt64)
ENGINE = ReplacingMergeTree(kafka_offset) ORDER BY trade_id;

CREATE TABLE serving.demo_unversioned (trade_id String, mtm Int32, kafka_offset UInt64)
ENGINE = ReplacingMergeTree ORDER BY trade_id;

CREATE TABLE serving.demo_plain (trade_id String, mtm Int32, kafka_offset UInt64)
ENGINE = MergeTree ORDER BY trade_id;

-- Newest is offset 12, mtm 130. Offset 11 arrives last, as a replay would.
INSERT INTO serving.demo_versioned VALUES ('T-1', 100, 10);
INSERT INTO serving.demo_versioned VALUES ('T-1', 130, 12);
INSERT INTO serving.demo_versioned VALUES ('T-1', 110, 11);

INSERT INTO serving.demo_unversioned VALUES ('T-1', 100, 10);
INSERT INTO serving.demo_unversioned VALUES ('T-1', 130, 12);
INSERT INTO serving.demo_unversioned VALUES ('T-1', 110, 11);

INSERT INTO serving.demo_plain VALUES ('T-1', 100, 10);
INSERT INTO serving.demo_plain VALUES ('T-1', 130, 12);
INSERT INTO serving.demo_plain VALUES ('T-1', 110, 11);

SELECT 'A. versioned, no FINAL' AS test, count() AS rows, sum(mtm) AS total_mtm FROM serving.demo_versioned;
SELECT 'B. versioned, FINAL' AS test, count() AS rows, sum(mtm) AS total_mtm FROM serving.demo_versioned FINAL;
SELECT 'C. unversioned, FINAL' AS test, count() AS rows, sum(mtm) AS total_mtm FROM serving.demo_unversioned FINAL;

-- The newest message delivered a second time.
INSERT INTO serving.demo_versioned VALUES ('T-1', 130, 12);
INSERT INTO serving.demo_plain VALUES ('T-1', 130, 12);

SELECT 'D. versioned after duplicate, FINAL' AS test, count() AS rows, sum(mtm) AS total_mtm FROM serving.demo_versioned FINAL;
SELECT 'E. plain MergeTree after duplicate' AS test, count() AS rows, sum(mtm) AS total_mtm FROM serving.demo_plain;

-- Force the merge that would normally happen whenever ClickHouse chose.
OPTIMIZE TABLE serving.demo_versioned FINAL;

SELECT 'F. versioned after merge, no FINAL' AS test, count() AS rows, sum(mtm) AS total_mtm FROM serving.demo_versioned;

DROP TABLE serving.demo_versioned;
DROP TABLE serving.demo_unversioned;
DROP TABLE serving.demo_plain;
