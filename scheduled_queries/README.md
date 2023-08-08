# Scheduled ADH queries using API and CF

## Short description
Connection with ADH using a CF to schedule a query allocated in ADH. 

## Long description
As Ads Data Hub (ADH() does not have a native scheduler that allows it to run a query, we have to build it using the ADH API and a Cloud Function (CF). Then the schedule could be done using Cloud Scheduler or any other GCP service to trigger the CF. 

The solution is in `main.py`. In this case we use a CF triggered by HTTP, you can change it according to your own needs. For the sake of simplicity, the code was written with the variables in the code. It is a best practice to allocate these variables as Environment variables, so they are not exposed in the code. There is a block code commented where you can find the code to retrieve these Environment Variables.

In this case, we already have a query written in ADH so the CF is just triggering it. There is also the possibility of writing the query in the python code. Maybe it will be addressed in the future. Or maybe not. We'll see.

Below we will explain some of the needed parameters to run this code and where to find them.


- GOOGLE_DEVELOPER_KEY 
  - idk yet. Will be updated.

- DISCOVERY_URL
  - It is not a variable. It's static as in the code if you're using the v1 version.

- ADH_CUSTOMER_ID
  - idk yet. Probably user ID in ADH, but I don't have enough permission to get it. Will be updated.
  -  If not specified, defaults to the customer that owns the query.

- QUERY_ID:
  - This ID can be extracted from the last part of the query's URL.
  ![alt text](images/query_id.jpeg)
  
- TIMEZONE
  - [Here](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) you have a list of all the potential timezones. This parameter is used as input in ADH when you press the Run Button.


The function _adh_wait_for_job_to_complete_ is not necessary. It is just a function to pause the code until the background job ends. The output of this function will be used then to make sure if the job finished or not. If it did not finish we can execute an action to rerun the CF until it finishes successfully.

  
  
