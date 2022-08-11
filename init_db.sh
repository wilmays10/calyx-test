sudo -u postgres psql << EOF
CREATE DATABASE calyx;
CREATE USER calyx_dba WITH PASSWORD 'calyx';
ALTER ROLE calyx_dba SET client_encoding TO 'utf8';
ALTER ROLE calyx_dba SET default_transaction_isolation TO 'read committed';
ALTER ROLE calyx_dba SET timezone TO 'UTC';
GRANT ALL PRIVILEGES ON DATABASE calyx TO calyx_dba;
\q
EOF