# Nasa-Log-Analytics
Processing NASA webserver logs with pyspark on AWS EMR. Tableau dashboard is created to generate insights from processed data.

## Architecture
![alt text](https://github.com/surjits254/Job-Scraping/blob/master/img/WebScraping_Architecture.jpg?raw=true)



## Below are steps to install this project:

### Step 1 : Installing Scraping Code on EC2 and Setting AWS S3 Bucket
<p> 1. Launch and EC2 instance and attach IAM role for putting objects on S3 <br>
    2. Edit S3 bucket_name and output_file variable in conf.ini file <br>
    3. Install scrapy library on EC2 <br>
    4. Execute below code: <br>
        <code> $ cd Job-Scraping/craiglist/craiglist </code> <br> 
        <code> $ scrapy crawl craigSpider -o <output_file you entered>.csv </code> </p>
        
### Step 2 : Setup Lambda Function
<p> 1. Create a lambda functiion with S3 event trigger. <br>
    2. Create signed URL for your output file and edit the load command inside lambda_function.py. <br>
    3. Edit the authorization value, which is base64 encoding on username:password value for connecting to neo4j. <br>
    4. Edit the IP address of Neo4j EC2 instance. </p>

### Step 3 : Setup Neo4j Graph Database on EC2
<p> 1. Launch new ec2 instance for neo4j graph database. <br>
    2. Refer this link --> https://dzone.com/articles/how-deploy-neo4j-instance for setting up neo4j on ec2 instace. <br>
    3. Make sure to open required inbound ports for ec2 security group providing access to ec2 for REST apis </p>

### Step 4 : Setup Flask REST APIs on EC2
<p> 1. Launch new ec2 instance for flask REST apis. <br>
    2. Edit ip address for neo4j ec2 instance and authorzation variable for neo4j in conf_flask.ini file. <br>
    3. open port 5000 on ec2 security group provding access to ec2 of angular UI. </p>

### Step 5 : Setup Angular UI on EC2
<p> 1. Launch an EC2 instance for Angular and open port 4200. <br>
    2. Install nvm using below command: <br>
        <code>  $ curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.35.3/install.sh | bash </code><br>
         <code> $ nvm install node </code> <br>
    3. Execute below commands to install and start apache web server: <br>
        <code> $ sudo yum -y install httpd </code><br>
        <code> $ sudo service httpd start </code><br>
    4. Change the ip address in src/app/job-service.service.ts file with ip address of EC2 where flask apis are running.<br>
    5. Execute this command to launch UI <code> $ ng serve --host 0.0.0.0 --port 4200 </code>
