# import statements
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import log
from pyspark.sql.functions import desc
from pyspark.sql.functions import udf
from pyspark.sql.functions import date_format, col
from pyspark.sql import SparkSession



spark = SparkSession.builder.appName("Nasa Log Analytics").getOrCreate()




class nasaLogAnalytics:
    
    def __init__(self):
        self.file_path = "s3://nasa-log-analytics/input-data/"
        self.host_regex = r'(^\S+\.[\S+\.]+\S+)\s'
        self.ts_regex = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}) -\d{4}]'
        self.method_uri_protocol_regex = r'\"(\S+)\s(\S+)\s*(\S*)\"'
        self.status_regex = r'\s(\d{3})\s'
        self.content_size_regex = r'\s(\d+)$'
        self.output_path = "s3://nasa-log-analytics/output-data/"
        self.output_files = {"status_code":"status_code_counts.csv","top_20_hosts":"top_20_hosts.csv","top_20_endpoints":"top_20_endpoints.csv",     "top_20_error_endpoints":"top_20_error_endpoints","hosts_per_day":"hosts_per_day.csv","host_404_per_day":"host_404_per_day.csv","endpoint_404_per_day":"endpoint_404_per_day.csv"}        
        
    def preProcess(self,udf_parse_time):
      
      # Loading data
      raw_data_df = spark.read.format("text").load(self.file_path).withColumnRenamed("value","raw")
      
      # using regex to extract columns from unstructered data
      extracted_df = raw_data_df.select(regexp_extract('raw',self.host_regex,1).alias('host'),\
                                 regexp_extract('raw',self.ts_regex,1).alias('timestamp'),\
                                 regexp_extract('raw',self.method_uri_protocol_regex,1).alias('method'),\
                                 regexp_extract('raw',self.method_uri_protocol_regex,2).alias('endpoint'),\
                                 regexp_extract('raw',self.method_uri_protocol_regex,3).alias('protocol'),\
                                 regexp_extract('raw',self.status_regex,1).alias('status').cast('Integer'),\
                                 regexp_extract('raw',self.content_size_regex,1).alias('content_size').cast('Integer'))
      
      # Removing nulls from content_size column and chaning data type of status and contentsize column to int
      extracted_df = extracted_df.where('status is not null').selectExpr('host','timestamp','method','endpoint','protocol','cast(status as int)','case when content_size is null then 0 else cast(content_size as int) end as content_size')
      
      # creating udf for trasnforming timestamp column
      
      
      # using parse_clf_time udf
      extracted_df = extracted_df.select('*', udf_parse_time('timestamp').cast('timestamp').alias('time')).drop('timestamp')
      
      # caching our dataframe for future use
      extracted_df = extracted_df.cache()
      
      print("done here")
      return extracted_df
        
    def loadTargetData(self,extracted_df):
      
      # status code analysis --> find count of each unique staus
      status_code_df = extracted_df.groupby('status').count().withColumn("log(count)",log("count"))
      print("done here")
      status_code_df.repartition(1).write.format("csv").mode('overwrite').option("header", "true").save(self.output_path+self.output_files["status_code"])
      print("done here")
      
      # finding top 20 host which accessed the server the most
      host_count_df = extracted_df.groupBy("host").count()
      top_20_hosts = host_count_df.select("host","count").orderBy(desc("count")).limit(20)
      top_20_hosts.repartition(1).write.format("csv").mode('overwrite').option("header", "true").save(self.output_path+self.output_files["top_20_hosts"])
      
      # finding top 20 endpoints
      endpoint_count_df = extracted_df.groupBy("endpoint").count()
      top_20_endpoints = endpoint_count_df.select("endpoint","count").orderBy(desc("count")).limit(20)
      top_20_endpoints.repartition(1).write.format("csv").mode('overwrite').option("header", "true").save(self.output_path+self.output_files["top_20_endpoints"])
      
      # top 20 error endpoints
      error_endpoint_df = extracted_df.where('status != 200').groupBy("endpoint").count()
      top_20_error_endpoints = error_endpoint_df.select("endpoint","count").orderBy(desc("count")).limit(20)
      top_20_error_endpoints.repartition(1).write.format("csv").mode('overwrite').option("header", "true").save(self.output_path+self.output_files["top_20_error_endpoints"])
      
      # total hosts per day
      hosts_per_day = extracted_df.select('host',date_format(col('time'),"yyyy-MM-dd").alias("date")).groupBy('host','date').count()
      hosts_per_day.repartition(1).write.format("csv").mode('overwrite').option("header", "true").save(self.output_path+self.output_files["hosts_per_day"])
      
      error_404_df = extracted_df.where('status = 404')
      
      # hosts with 404 each day
      host_404_per_day = error_404_df.select('host',date_format(col('time'),"yyyy-MM-dd").alias("date")).groupBy('host','date').count()
      host_404_per_day.repartition(1).write.format("csv").mode('overwrite').option("header", "true").save(self.output_path+self.output_files["host_404_per_day"])
      
      # endpoints with 404 each day
      endpoint_404_per_day = error_404_df.select('endpoint',date_format(col('time'),"yyyy-MM-dd").alias("date")).groupBy('endpoint','date').count()
      endpoint_404_per_day.repartition(1).write.format("csv").mode('overwrite').option("header", "true").save(self.output_path+self.output_files["endpoint_404_per_day"])
      
      return "files loaded successfully"
        


def parse_clf_time(st):
    
    month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(int(st[7:11]),self.month_map[st[3:6]],int(st[0:2]),int(st[12:14]),int(st[15:17]),int(st[18:]))
    
udf_parse_time = udf(parse_clf_time)

      
nla = nasaLogAnalytics()
processedDf = nla.preProcess(udf_parse_time)
finalResult = nla.loadTargetData(processedDf)
print(finalResult)