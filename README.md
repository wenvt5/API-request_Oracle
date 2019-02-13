# API-request_Oracle
automation of API request and results storing in Oracle

## Prepare request
- retrieve login info from csv file and log in production database
- query from production database
- parse the query result to the right format for API request

## send requests
- Create a connect session and make a request with the parsed query results

## save results
- Parse the returned result from Json to dataframe
- Save the result as excel
- retrieve the login info from csv file and log in stage database
- Save the result in stage database 
