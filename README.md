# Nasa-Log-Analytics
Processing NASA webserver logs with pyspark on AWS EMR. Tableau dashboard is created to generate insights from processed data.

## Architecture
![alt text](https://github.com/surjits254/Nasa-Log-Analytics/blob/master/img/nasa_log_analytics_architecture.png?raw=true)

## Tableau Dashboard
### Public URL --> <a> https://public.tableau.com/profile/surjit.singh4117#!/vizhome/Nasa_Log_Analytics/LogAnalytics?publish=yes </a>
![alt text](https://github.com/surjits254/Nasa-Log-Analytics/blob/master/img/nasa_log_analytics_tableau.png?raw=true)


## Follow below steps to run this project:

1. Create S3 bucket, unzip and upload input data files also nasaLogAnalytics.py to bucket. Change the python file according to your s3 bucket name. See below picture for S3 folder structure:
![alt text](https://github.com/surjits254/Nasa-Log-Analytics/blob/master/img/s3.PNG?raw=true) <br>
2. Create EMR cluster and execute ```aws s3 cp s3://<your bucket name>/code/nasaLogAnalytics.py .``` to copy the code from s3 to emr. <br>
3. Run ```spark-submit nasaLogAnalytics.py``` to execute the code and later consume output files from S3.
