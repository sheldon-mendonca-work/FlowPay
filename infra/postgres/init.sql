SELECT 'CREATE DATABASE payment_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'payment_db')\gexec
-- SELECT 'CREATE DATABASE ledger_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ledger_db')\gexec
-- SELECT 'CREATE DATABASE wallet_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'wallet_db')\gexec
-- SELECT 'CREATE DATABASE offer_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'offer_db')\gexec
-- SELECT 'CREATE DATABASE fraud_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'fraud_db')\gexec
-- SELECT 'CREATE DATABASE reconciliation_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'reconciliation_db')\gexec

