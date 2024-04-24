from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
fire_ts_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)





import pyspark.sql.functions as F
fire_ts_df = fire_ts_df.withColumnRenamed("Delay", "ResponseDelayedInMins")
fire_ts_df \
    .select(
        F.sum("NumAlarms"), 
        F.avg("ResponseDelayedInMins"), 
        F.min("ResponseDelayedInMins"), 
        F.max("ResponseDelayedInMins"),
	F.variance("ResponseDelayedInMins")
    ) \
    .show()





fire_ts_df \
    .select("CallType") \
    .where(col("CallType").isNotNull()) \
    .groupBy("CallType") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(n=10, truncate=False)

from pyspark.sql.functions import avg

# 소방서 지역별로 평균 응답 지연 시간과 사건 수를 계산
fire_ts_df \
    .groupBy("StationArea") \
    .agg(
        avg("Delay").alias("Average_Delay"),
        count("*").alias("Total_Calls")
    ) \
    .orderBy(col("Total_Calls").desc())
    .show(10)
# 소방서 지역별로 평균 응답 지연 시간과 사건 수를 계산
# 모든 행을 세어 총 호출 수를 계산
# 사건 수를 기준으로 내림차순 정렬

fire_ts_df \
  .filter(col("ALSUnit")).groupBy("Neighborhood") \
  .agg(_sum("NumAlarms").alias("Total_Alarms")) \
  .orderBy(col("Total_Alarms").desc()) \
  .show(10)

fire_ts_df \
    .groupBy("Location", "CallType") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10)


 'CallType' 컬럼만 선택
#'CallType' 컬럼에서 null이 아닌 값들만 필터링합니다.
#'CallType' 값에 따라 데이터를 그룹화합니다.
#각 그룹의 데이터 수를 세어 새로운 컬럼으로 추가합니다.  컬럼 이름은 기본적으로 'count'
# 'count' 컬럼을 기준으로 결과를 내림차순 정렬
# 상위 10개의 결과를 전체 텍스트를 보여주며 출력



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



