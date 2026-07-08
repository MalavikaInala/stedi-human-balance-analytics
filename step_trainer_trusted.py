import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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
AmazonS3_node1783421561674 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="AmazonS3_node1783421561674")

# Script generated for node Amazon S3
AmazonS3_node1783421565891 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="AmazonS3_node1783421565891")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1783421726984 = ApplyMapping.apply(frame=AmazonS3_node1783421565891, mappings=[("serialnumber", "string", "right_serialnumber", "string"), ("z", "double", "right_z", "double"), ("`.customername`", "string", "`right_.customername`", "string"), ("birthday", "string", "right_birthday", "string"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("`.email`", "string", "`right_.email`", "string"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("registrationdate", "long", "right_registrationdate", "long"), ("customername", "string", "right_customername", "string"), ("`.phone`", "string", "`right_.phone`", "string"), ("`.sharewithpublicasofdate`", "long", "`right_.sharewithpublicasofdate`", "long"), ("user", "string", "right_user", "string"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"), ("y", "double", "right_y", "double"), ("`.birthday`", "string", "`right_.birthday`", "string"), ("`.lastupdatedate`", "long", "`right_.lastupdatedate`", "long"), ("`.sharewithfriendsasofdate`", "long", "`right_.sharewithfriendsasofdate`", "long"), ("x", "double", "right_x", "double"), ("timestamp", "long", "right_timestamp", "long"), ("`.registrationdate`", "long", "`right_.registrationdate`", "long"), ("`.serialnumber`", "string", "`right_.serialnumber`", "string"), ("email", "string", "right_email", "string"), ("lastupdatedate", "long", "right_lastupdatedate", "long"), ("`.sharewithresearchasofdate`", "long", "`right_.sharewithresearchasofdate`", "long"), ("phone", "string", "right_phone", "string")], transformation_ctx="RenamedkeysforJoin_node1783421726984")

# Script generated for node Join
Join_node1783421620742 = Join.apply(frame1=AmazonS3_node1783421561674, frame2=RenamedkeysforJoin_node1783421726984, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1783421620742")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1783421620742, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783420651557", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1783421760203 = glueContext.write_dynamic_frame.from_options(frame=Join_node1783421620742, connection_type="s3", format="glueparquet", connection_options={"path": "s3://stedi-data-lake-malavika/step_trainer_trusted/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1783421760203")

job.commit()