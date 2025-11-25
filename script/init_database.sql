/*
=============================================================
Create Databases (MySQL)
=============================================================
WARNING:
Running this script will DROP the databases if they already exist.
Proceed carefully.
=============================================================
*/

-- Drop existing databases if they exist
DROP DATABASE IF EXISTS DataWarehouse;
DROP DATABASE IF EXISTS bronze;
DROP DATABASE IF EXISTS silver;
DROP DATABASE IF EXISTS gOLD;

-- Create DataWarehouse database (main container)
CREATE DATABASE DataWarehouse;

-- Create schema-equivalent databases
CREATE DATABASE bronze;
CREATE DATABASE silver;
CREATE DATABASE gold;

-- Optional: select a database to use
USE DataWarehouse;
