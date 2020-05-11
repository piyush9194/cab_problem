#  Details about the approach taken to solve the problem

1. To find the location of the driver the first we create a polygon from the list of latitude nad longitudes provided
   in the districts json assuming that the coordinates are in sequence and there is no hole in the poLygon.
   
   Now we chek the driver coordinates lies in which ploygon. As soon as the match is found we break the loop and assign
   the district and city details to the driver.
   
2. The merged dataset is then rolled up at the desired date, hour, five_minutes,ditrict, city and driver status levels.

3. The final output is created in both json and parquet formats in the folders outpu_json_file and output_parquet_file
   in the project folder itself.     



#  Definition of supply

The count of drivers rolled up the required levels is the definition of supply at a given point in 
time and space.


#  Possible improvements in the spark ETL

1. We can further optimize the way in whcih we are checking the driver's location. The for loop is not optimized
   and we can look for other aproaches.

2. We can rewrite the ETL to handle real time data ingestion by using spark streaming. 


## Instructions to run the application from command line
```
         ./bin/spark-submit 
         --master yarn 
         --deploy-mode cluster \ # can be client for client mode --driver-memory 5g 
         –-num-executors 10 
         --executor-memory 4g 
         /Users/piygupta/Desktop/Learning/DATA_ENGINEER_TEST/etl.py 

         The parameters num-executors, num-executors ,executor-memory can be fine tuned based on the load type.
```
#  Sample command to run from command line

spark-submit --deploy-mode client --num-executors 1 /Users/piygupta/Desktop/Learning/DATA_ENGINEER_TEST/etl.py 
