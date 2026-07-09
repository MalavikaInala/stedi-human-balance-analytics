import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1783538880475 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AmazonS3_node1783538880475")

# Script generated for node Amazon S3
AmazonS3_node1783538878573 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="AmazonS3_node1783538878573")

# Script generated for node SQL Query
SqlQuery3575 = '''
SELECT DISTINCT c.serialnumber, c.customername, c.email, c.phone, c.birthday, c.registrationdate, c.lastupdatedate, c.sharewithpublicasofdate, c.sharewithfriendsasofdate, c.sharewithresearchasofdate FROM c INNER JOIN a ON c.email = a.user
'''
SQLQuery_node1783538928676 = sparkSqlQuery(glueContext, query = SqlQuery3575, mapping = {"c":AmazonS3_node1783538878573, "a":AmazonS3_node1783538880475}, transformation_ctx = "SQLQuery_node1783538928676")

# Script generated for node SQL Query
SqlQuery3576 = '''
select * from myDataSource

'''
SQLQuery_node1783539124733 = sparkSqlQuery(glueContext, query = SqlQuery3576, mapping = {"myDataSource":SQLQuery_node1783538928676}, transformation_ctx = "SQLQuery_node1783539124733")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783539124733, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783536599921", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1783539139702 = glueContext.getSink(path="s3://stedi-data-lake-malavika/customers_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1783539139702")
AmazonS3_node1783539139702.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
AmazonS3_node1783539139702.setFormat("glueparquet", compression="snappy")
AmazonS3_node1783539139702.writeFrame(SQLQuery_node1783539124733)
job.commit()