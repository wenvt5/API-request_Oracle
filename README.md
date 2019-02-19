# API-request_Oracle
automation of API request and results storing in Oracle

This program has the following functions:
1. read login info from a file and make connection to a specified Oracle database
2. query data from the connection and reformat it to suit API requests
3. make the api request with the data prepared from the previous step
4. save the retured excel file
5. parse the result to dataframe matching database table
6. write the dataframe format of result to Oracle datatable


# The following functions have been refactored to python class "OracleManager" in read_write_Oracle.py:
1. read login info from a file and make connection to a specified Oracle database
2. query data from the connection
3. write the dataframe to Oracle datatable
