CREATE DATABASE raonnydbprod;
CREATE USER dbadminprod WITH PASSWORD 'Senha.raonny4827';
ALTER ROLE dbadminprod SET client_encoding TO 'utf8';
ALTER ROLE dbadminprod SET default_transaction_isolation TO 'read committed';
ALTER ROLE dbadminprod SET timezone TO 'UTC';
GRANT ALL PRIVILEGES ON DATABASE raonnydbprod TO dbadminprod;
GRANT ALL PRIVILEGES ON SCHEMA public TO dbadminprod;
GRANT postgres TO dbadminprod;
\q