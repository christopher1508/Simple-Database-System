# Simple-Database-System
This code implements a simple DBMS system. This DBMS will allow the user to type in simple DDL statements and build a system catalog (set of packed descriptors).  Once the table definition is in place, the user can insert, update, delete, and select from a table.

Data type supports: INT, CHAR
<aggregate> can be SUM, AVG, COUNT

Command Syntax:
  1. CREATE TABLE table_name (  { column_name <data_type> [NOT NULL] }  )
  2. DROP TABLE table_name
  3. LIST TABLE
  4. LIST SCHEMA FOR table_name [TO report_filename]
  5. INSERT INTO table_name VALUES (  { data_value }  )
  6. DELETE FROM table_name [ WHERE column_name <relational_operator> data_value ]
  7. UPDATE table_name SET column = data_value [ WHERE column_name <relational_operator> data_value ]
  8. SELECT { column_name } FROM table_name
	    [ WHERE column_name <condition> [(AND | OR) column_name <condition>] ]
      [ ORDER BY column_name [DESC] ] || 
	   SELECT <aggregate>(column_name) FROM table_name
	    [ WHERE column_name <condition> [(AND | OR) column_name <condition>] ]
      [ ORDER BY column_name [DESC] ]
  9. BACKUP TO <image file name>
  10. RESTORE FROM <image file name> [WITHOUT RF]
  11. ROLLFORWARD [TO <timestamp>]
