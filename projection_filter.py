from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Count").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")  

# 스키마 정의
fire_schema = StructType([
    StructField('CallNumber', IntegerType(), True), 
    StructField('UnitID', StringType(), True), 
    StructField('IncidentNumber', IntegerType(), True), 
    StructField('CallType', StringType(), True), 
    StructField('CallDate', StringType(), True), 
    StructField('WatchDate', StringType(), True), 
    StructField('CallFinalDisposition', StringType(), True), 
    StructField('AvailableDtTm', StringType(), True), 
    StructField('Address', StringType(), True), 
    StructField('City', StringType(), True), 
    StructField('Zipcode', IntegerType(), True), 
    StructField('Battalion', StringType(), True), 
    StructField('StationArea', StringType(), True), 
    StructField('Box', StringType(), True), 
    StructField('OriginalPriority', StringType(), True),
    StructField('Priority', StringType(), True), 
    StructField('FinalPriority', IntegerType(), True), 
    StructField('ALSUnit', BooleanType(), True), 
    StructField('CallTypeGroup', StringType(), True), 
    StructField('NumAlarms', IntegerType(), True), 
    StructField('UnitType', StringType(), True), 
    StructField('UnitSequenceInCallDispatch', IntegerType(), True), 
    StructField('FirePreventionDistrict', StringType(), True), 
    StructField('SupervisorDistrict', StringType(), True), 
    StructField('Neighborhood', StringType(), True), 
    StructField('Location', StringType(), True), 
    StructField('RowID', StringType(), True), 
    StructField('Delay', FloatType(), True)
])

# CSV 파일 경로
sf_fire_file = r"C:\Users\LG\anaconda3\Scripts\sf-fire-calls.csv" 

# CSV 파일을 읽어서 DataFrame 생성
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

from pyspark.sql.functions import col

#파이썬 예제 
few_fire_df = fire_df.select("IncidentNumber", "AvailableDtTm", "CallType") \
                     .where(col("CallType") != "Medical Incident")

few_fire_df.show(5, truncate=False)

from pyspark.sql.functions import *

fire_df.select("CallType") \
    .where(col("CallType").isNotNull()) \
    .agg(countDistinct("CallType").alias("DistinctCallTypes")) \
    .show()
  # CallType 열을 선택
  # CallType이 null이 아닌 행을 선택
  # CallType 열의 고유한 값 수를 집계
  # 결과를 출력합니다

fire_df.select("CallType") \
    .where(col("CallType").isNotNull()) \
    .distinct() \
    .show(10, False) 
  # CallType 열을 선택
  # CallType이 null이 아닌 행을 선택
  # 중복을 제거
 # 결과를 출력합니다. 최대 10개의 행을 출력하고, 열 잘라내지 않음.



