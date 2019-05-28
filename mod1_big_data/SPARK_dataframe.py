'''
MACHINE LEARNING PARA ANÁLISE DE DADOS
CESAR SCHOOL
Big Data

@claudio alves monteiro
maio/2019
'''

#==========================================#
# import modules and set environment
#========================================#

# paths to spark and python3
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--executor-memory 1G pyspark-shell'
os.environ["SPARK_HOME"] = "/home/pacha/spark"
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"

# execute PYSPARK
exec(open('/home/pacha/spark/python/pyspark/shell.py').read())

#==============================#
# SPARK DATA
#============================#

# create spark data
iphones_RDD = sc.parallelize([
("XS", 2018, 5.65, 2.79, 6.24),
("XR", 2018, 5.94, 2.98, 6.84),
("X10", 2017, 5.65, 2.79, 6.13),
("8Plus", 2017, 6.23, 3.07, 7.12)
])
names = ['Model', 'Year', 'Height', 'Width', 'Weight']

iphones_df = spark.createDataFrame(iphones_RDD, schema=names)
type(iphones_df)

# create Spark DataFrame from CSV/JSON/TXT
#df_json = spark.read.json("file.json", header=True, inferSchema=True)
#df_txt = spark.read.txt("file.txt", header=True, inferSchema=True)
df = spark.read.csv("mod1_big_data/atp_matches_2018.csv", header=True, inferSchema=True)

#==============================#
# VIEW AND SELECT SPARK DATA
#============================#

# printSchema() - print all column's types
df.printSchema()

# select()
df_w_age = df.select('winner_age')

# show() - first 20 lines as standard
df_w_age.show(10)

# filter() - filter as lines based in condition
df_w_age35 = df_w_age.filter(df_w_age.winner_age > 35)
df_w_age35.show(10)

# groupby() - usado para agrupar uma variável
df_country_group = df_csv.groupby('winner_ioc')
df_country_group.count().show(5)

# orderBy()
df_country_group.count().orderBy('count', ascending=False).show(5)

# withColumnRenamed() - renomeia uma coluna no DataFrame
df_country = df_csv.withColumnRenamed('winner_ioc', 'winner_country')
df_country.select('winner_country', 'winner_name').show(5)

# columns
df_country.columns

# describe() - mostra as principais estatísticas dos campos numéricos no DataFrame
df_country.select('winner_name', 
                  'winner_country', 
                  'winner_age', 
                  'w_ace').describe().show()

