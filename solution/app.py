
import os
import logging
import time
from pendulum import now
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as SDF
from pyspark.sql.types import StructType, ArrayType, MapType
from pyspark.sql.functions import from_json, col, explode_outer, map_keys, from_json, lit, avg, count, round, sum, when, size
from zipfile import ZipFile
from pathlib import Path


# Logger
logging.basicConfig(
    format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
logger = logging.getLogger('driver_logger')
logger.setLevel(logging.DEBUG)

# Create SparkSession
spark = (SparkSession
         .builder
         .appName("Challenge")
         .master("local[*]")
         .getOrCreate())


def flatten(df: SDF, sep="_") -> SDF:
    """ Flatten dataframe.

    Args:
        df (SDF): Spark dataframe
        sep (str, optional): Default column separator. Defaults to "_".

    Returns:
        SDF: Returns a flattened Spark dataframe
    """

    # compute Complex Fields (Arrays, Structs and Maptypes) in Schema
    complex_fields = dict(
        [
            (field.name, field.dataType)
            for field in df.schema.fields
            if type(field.dataType) == ArrayType
            or type(field.dataType) == StructType
            or type(field.dataType) == MapType
        ]
    )

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        # print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if type(complex_fields[col_name]) == StructType:
            expanded = [
                col(col_name + "." + k).alias(col_name + sep + k)
                for k in [n.name for n in complex_fields[col_name]]
            ]
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))

        # if MapType then convert all sub element to columns.
        # i.e. flatten
        elif type(complex_fields[col_name]) == MapType:
            keys_df = df.select(explode_outer(
                map_keys(col(col_name)))).distinct()
            keys = list(map(lambda row: row[0], keys_df.collect()))
            key_cols = list(
                map(
                    lambda f: col(col_name).getItem(
                        f).alias(str(col_name + sep + f)),
                    keys,
                )
            )
            drop_column_list = [col_name]
            df = df.select(
                [
                    col_name
                    for col_name in df.columns
                    if col_name not in drop_column_list
                ]
                + key_cols
            )

        # recompute remaining Complex Fields in Schema
        complex_fields = dict(
            [
                (field.name, field.dataType)
                for field in df.schema.fields
                if type(field.dataType) == ArrayType
                or type(field.dataType) == StructType
                or type(field.dataType) == MapType
            ]
        )

    return df


def unzip_file(file_path: str, file: str) -> None:
    """Unzip file on provided path

    Args:
        file_path(str): Path to folder holding zip file
        file(str): filename to unzipdf.

    """
    source_dir = os.path.join(file_path, file)

    with ZipFile(source_dir, 'r') as zObject:
        zObject.extractall(file_path)


def explode_user_token(df: SDF) -> SDF:
    # Get Schema from Token Column
    json_col_list = df.select("user_token").rdd.flatMap(lambda x: x)
    schema_ddl = spark.read.json(json_col_list)._jdf.schema()
    # Read column with proper schema
    mapped_df = df.withColumn("user_token",
                              from_json("user_token", schema=schema_ddl))
    # Select all columns
    parsed_df = (mapped_df
                 .select("*", col("user_token.*"))
                 .drop(col("user_token")))

    return parsed_df


def get_data(path_to_json: str) -> SDF:

    logger.info("Getting JSON files")
    # Read Json files
    df = spark.read.json(path=path_to_json)
    # Flatten Dataframe
    logger.info("Flattening Dataframe")
    df_flattened = flatten(df)
    # Explode User Token
    logger.info("Explode User Token")
    parsed_df = explode_user_token(df_flattened)
    # Deduplication of events based on event ID
    # < Challenge request >
    logger.info("Dedup data")
    parsed_df = parsed_df.dropDuplicates(['datetime', 'id'])

    return parsed_df


def get_consent(df: SDF) -> SDF:
    """Check if user has at least one purposed enabled and if so set 
    the user_consent as True

    Returns:
        SDF: Spark dataframe
    """
    df = (df.withColumn("user_consent",
                        when((size(col("purposes.enabled")) > 0), True)
                        .otherwise(False)))

    return df


def get_metrics(df: SDF) -> SDF:
    # Output Dataframe with all metrics
    df_output = spark.createDataFrame([], StructType([]))
    # Challenge key
    data_key = ["datehour", "domain", "user_country"]

    """
      PAGE VIEW
    """
    logger.info("Getting Page View metrics")
    # Group data for Page Views
    df_pv = (df.filter(col('type') == 'pageview')
             .groupBy(data_key + ["user_id"])
             .agg(count(col("id")).alias("qtd_views"),
                  sum(when((col("user_consent") == True), 1).otherwise(0)).alias('pageviews_with_consent')))

    # Metrics for page views according keys defined
    df_views = (df_pv
                .groupBy(data_key)
                .agg(sum(col("qtd_views")).alias("pageviews"),
                     sum(col("pageviews_with_consent")).alias(
                    "pageviews_with_consent"),
                    round(avg("qtd_views"), 2).alias("avg_pageviews_per_user")))

    """
      CONSENTS

    """
    logger.info("Getting Consents metrics")
    # Group data for Page Views
    df_pv = (df.groupBy(data_key)
             .agg(sum(when((col("type") == 'consent.asked'), 1).otherwise(0)).alias('consents_asked'),
                  sum(when((col("type") == 'consent.asked') & (
                      col("user_consent") == True), 1).otherwise(0)).alias('consents_asked_with_consent'),
                  sum(when((col("type") == 'consent.given'),
                      1).otherwise(0)).alias('consents_given'),
                  sum(when((col("type") == 'consent.given') & (
                      col("user_consent") == True), 1).otherwise(0)).alias('consents_given_with_consent'),
                  ))

    logger.info("Joining Page View x Consents - metrics")
    df_output = (df_views
                 .join(df_pv, data_key, how="left")
                 .select(*data_key,
                         col("pageviews"),
                         col("pageviews_with_consent"),
                         col("consents_asked"),
                         col("consents_asked_with_consent"),
                         col("consents_given"),
                         col("consents_given_with_consent"),
                         col("avg_pageviews_per_user"))
                 .withColumnRenamed("user_country", "country"))

    return df_output


def write_df_as_parquet(df: SDF, target_dir: str) -> None:

    # Write Dataframe
    file_timestamp = now().format('YYYYMMDD_HHmmss')
    filename = f"challenge_{file_timestamp}.parquet"
    df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(f'{target_dir}/{filename}')


def main():

    logger.info("Starting - Challenge")
    # Starting execution
    start_time = time.time()

    # Get file to extract
    path_to_unzip = os.path.join(Path(os.getcwd()), 'data')
    filename = "input-example.zip"
    # Decompress file
    unzip_file(file_path=path_to_unzip, file=filename)

    path_to_raw = os.path.join(path_to_unzip, 'input')
    # Get JSON data
    df = get_data(path_to_raw)
    # Check if consent exists otherwise create column
    df = get_consent(df)
    # Metrics
    df = get_metrics(df)
    df.show()
    # Write Dataframe
    write_df_as_parquet(df, path_to_unzip)

    logger.info(f"Elapsed time: {(time.time() - start_time)} seconds.")
    logger.info("End - Challenge")


if __name__ == '__main__':
    main()
