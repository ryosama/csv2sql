csv2sql
==========
Request CSV files like SQL databases

How it works
============
The script convert the CSV file into a SQLite database and request this database with SQL command.
Then it displays the result like others SQL request tools.

Usage
=====
--sql=select ... from csv_file.csv ...

--dont-create
	Do not recreate the sqlite database, only request the existing one

--delimiter=;
	Delimiter for CSV file. Default caracter is ;

--usage ou --help
	Display this message

--debug
	Display more information about the process

Example
=======
perl csv2sql.pl "--sql=SELECT * FROM my_records.csv WHERE seller_name LIKE 'bob%'"