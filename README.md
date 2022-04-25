# SQL Query Database Engine in Python

Uses [sqlparse](https://github.com/andialbrecht/sqlparse) and [sql-metadata](https://github.com/macbre/sql-metadata) parsers to evaluate the SQL queries and print the result. Some additional features of SQL parser and translator are created in python for this project.

Implemented SQL Operations
- SELECT
- INNER JOIN
- WHERE
- LIMIT
- AGGREGATION (SUM, MIN, MAX, AVG, COUNT)
- ALIAS

## Architecture

The database reads the data from **.dat** files. The **.dat** files contain a single tuple at each line, with each column separated by pipe ( **|** ) symbol. The name of the data files is read from the CREATE statement.  The SQL query must be given using **.sql** file that consists of CREATE statements at the beginning for all tables and SELECT queries in each line. The sample **.dat** files are given in this repository under the **data** folder. The sample **.sql** files are provided under the **queries** folder.

![alt text](https://github.com/rdpahalavan/sql-dbms-python/blob/main/images/SQL%20Query%20Engine.png)

The database reads one tuple at a time from the tables that are asked in the query. The tuples from different tables are merged into a single tuple based on the JOIN condition. Then, the columns in the tuple are compared for the WHERE condition, and if the comparison is met, the tuple is then printed to the output or stored in the memory for the final aggregation to be performed.

![alt text](https://github.com/rdpahalavan/sql-dbms-python/blob/main/images/Relational%20Algebra%20Tree.png)
