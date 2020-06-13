# SQL Parser

Utility to parse a list of stored procedures and extract SQL entities referenced alongwith the SQL operations against each.


### Usage / Installation

Clone git repo on local machine.

Edit the `storedprocs.txt` file in the project directory and add the stored procedure names to parse, one per line. It is
not necessary to specify the database name.  

Open command prompt, cd to debug or release directory and run the following command:
```
sqlparser c:\development\test-database .\storedprocs.txt
```
where folder `\test-database` contains the SQL script files to parse.

The results are written to a `results.csv` file in the executable directory with the following format:


|Dynamic SQL|DB Name|Stored Proc Name|Action|Entity|
|---|---|---|---|---|
|False|TestDB|Loan_Select|SELECT|TestDB.Loan|
|False|TestDB|Loan_Insert|SELECT|TestDB.Loan|
|False|TestDB|Loan_Insert|INSERT|TestDB.Loan|
|False|TestDB|Loan_Delete|DELETE|TestDB.Loan|
|False|ContentDB|Image_Delete|DELETE|ContentDB.tblPDF|
|False|ContentDB|Image_Delete|DELETE|ContentDB.tblJPG|
|False|ContentDB|Image_Delete|DELETE|ContentDB.tblSVG|


**Columns:**
```
Dynamic SQL - indicates if the stored procedure has any dynamic sql (the parser cannot handle this and it has to be looked at manually)
DB Name - the database that contains the stored procedure being parsed
Stored Proc Name - the name of the stored procedure being parsed 
Action - the SQL operation performed within the stored procedure being parsed
Entity - the SQL entity against which the action is being performed
```



### Dependencies
Nuget package Microsoft.SqlServer.DacFx


### Prerequisites
.NET Framework 4.7.2

 
