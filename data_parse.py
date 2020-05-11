from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import IntegerType

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .master('local')
             .appName('One Championship Data Load')
             .getOrCreate()
             )
    spark.sparkContext.setLogLevel("WARN")

    data_file_path = "/Users/ashgarg/Documents/repo/test_one_chapionship/input_data/data.csv"
    output_path = '/Users/ashgarg/Documents/repo/test_one_chapionship/output_data/'

    data = (spark
            .read
            .csv(data_file_path,
                 inferSchema=True,
                 header=True)
            )

    # person_id, datetime, floor_level, building
    data_per_schema = (data
                       .withColumn('datetime', to_timestamp('Floor Access DateTime', 'MM/dd/yy hh:mm'))
                       .withColumn('floor_level', data['Floor Level'].cast(IntegerType()))
                       .selectExpr("'Person Id' as person_id",
                                   'datetime',
                                   'floor_level',
                                   'Building as building'
                                   ))

    data.write.mode('overwrite').json(output_path)
