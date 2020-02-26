
-- create database first
CREATE DATABASE stockmarket;

-- move to database that has been created
\c stockmarket

-- execute file temp_project.sql
\i temp_project.sql

-- move again to database
\c stockmarket
  
-- check list of tables
\dt

-- create additional table to store result of average_price for each market in each month
CREATE TABLE resultsql (month VARCHAR, average_price VARCHAR, index VARCHAR);


----------------------------------------------------------  HK  ---------------------------------------------------

-- execute tables to get average price for each market for each month and then store to the new table that has been created
-- first process to get HK market
INSERT INTO resultsql(month, average_price) select day, AVG(price) from (select * from imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='HK') AS price  GROUP by day ORDER BY day;

-- update index for HK
UPDATE resultsql set index = 'HK';

----------------------------------------------------------  TWSE  ---------------------------------------------------

-- second process to get TWSE market
INSERT INTO resultsql(month, average_price) select day, AVG(price) from (select * from imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='TWSE') AS price  GROUP by day ORDER BY day;

-- update index for TWSE
UPDATE resultsql set index = 'TWSE' WHERE index IS NULL; 



----------------------------------------------------------  CSI300  ---------------------------------------------------

-- third process to get CSI300 market
INSERT INTO resultsql(month, average_price) select day, AVG(price) from (select * from imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='CSI300') AS price  GROUP by day ORDER BY day;

-- update index for CSI300
UPDATE resultsql set index = 'CSI300' WHERE index IS NULL;


----------------------------------------------------------  KOSPI2  ---------------------------------------------------

-- forth process to get KOSPI2 market
INSERT INTO resultsql(month, average_price) select day, AVG(price) from (select * from imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='KOSPI2') AS price  GROUP by day ORDER BY day;

-- update index for KOSPI2
UPDATE resultsql set index = 'KOSPI2' WHERE index IS NULL;

----------------------------------------------------------  Export to resultsql.csv  ---------------------------------------------------
-- assume, resultsql.csv is already exist in home and has permission already, if not create file resultsql.csv first and do "sudo chmod 777 resultsql.csv"

\COPY resultsql TO resultsql.csv WITH (FORMAT CSV, HEADER);



