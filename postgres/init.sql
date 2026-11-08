-- Reference data: the slow-changing facts that trades point to.
-- IDs must match simulators/common.py.

CREATE TABLE instruments (
    instrument_id text PRIMARY KEY,
    name          text NOT NULL,
    daily_vol     numeric(6, 4) NOT NULL  -- 0.0200 means a typical day moves about 2%
);

CREATE TABLE counterparties (
    counterparty_id text PRIMARY KEY,
    name            text NOT NULL,
    credit_limit    numeric(18, 2) NOT NULL  -- maximum exposure the bank accepts, in USD
);

CREATE TABLE books (
    book_id text PRIMARY KEY,
    name    text NOT NULL
);

INSERT INTO instruments VALUES
    ('AAPL',  'Apple Inc',                0.0160),
    ('MSFT',  'Microsoft Corp',           0.0140),
    ('NVDA',  'NVIDIA Corp',              0.0320),
    ('AMZN',  'Amazon.com Inc',           0.0200),
    ('GOOGL', 'Alphabet Inc',             0.0180),
    ('META',  'Meta Platforms Inc',       0.0240),
    ('TSLA',  'Tesla Inc',                0.0380),
    ('JPM',   'JPMorgan Chase & Co',      0.0150),
    ('XOM',   'Exxon Mobil Corp',         0.0160),
    ('KO',    'Coca-Cola Co',             0.0090);

-- Invented funds. A trade averages about $230k, so these limits take minutes to reach.
INSERT INTO counterparties VALUES
    ('CP-1', 'Alder Ridge Capital',   25000000.00),
    ('CP-2', 'Birchfield Partners',   50000000.00),
    ('CP-3', 'Cobalt Harbour Fund',   40000000.00),
    ('CP-4', 'Dunmore Macro',         15000000.00),
    ('CP-5', 'Eastgate Long/Short',   75000000.00);

INSERT INTO books VALUES
    ('BOOK-1', 'US Equities Flow'),
    ('BOOK-2', 'US Equities Hedge Funds'),
    ('BOOK-3', 'US Equities Inventory');

-- Debezium logs in as its own user, with only the rights it needs:
-- REPLICATION to read the WAL, SELECT to do the first snapshot.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
GRANT SELECT ON instruments, counterparties, books TO debezium;

-- Write the whole old row to the WAL, so change events carry a complete "before".
-- Stream processors need it to take the old value back out of their results.
ALTER TABLE instruments REPLICA IDENTITY FULL;
ALTER TABLE counterparties REPLICA IDENTITY FULL;
ALTER TABLE books REPLICA IDENTITY FULL;

-- A publication is the list of tables whose changes are sent to WAL readers.
CREATE PUBLICATION refdata_pub FOR TABLE instruments, counterparties, books;
