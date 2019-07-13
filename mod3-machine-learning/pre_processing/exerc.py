'''
MACHINE LEARNING PARA ANALISE DE DADOS
CESAR School

@Claudio Alves Monteiro
'''

# paths to spark and python3
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--executor-memory 1G pyspark-shell'
os.environ["SPARK_HOME"] = "/home/pacha/spark"
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"

# execute PYSPARK
exec(open('/home/pacha/spark/python/pyspark/shell.py').read())

# import spark functions
from pyspark.sql import functions as SF

# import data
df = spark.read.csv('titanic/train.csv', header=True, inferSchema=True)

# visualizar dados
df.show()
df.describe().show()
df.printSchema()

# remover colunas que nao agregam valor
df.filter(df['Cabin'].isNull()).count() # CRIAR LOOP PARA CONTAR NULOS NA BASE
df.filter(df['Cabin'].isNull()).count() / df.count()
df = df.drop('Cabin', 'Ticket', 'PassengerId', 'Name')

# tratar valores nulos substituindo pela mediana

media = df.select('Age').agg({"Age": "avg"})
mediana = df.selectExpr('percentile(Age, 0.5) as p')

mediana.show()

x = media.select("avg(Age)").rdd.flatMap(lambda x: x).collect()

df = df.fillna(x[0], subset=['Age'])

df.show()

# remover linhas que ainda tem nulos
df.dropna().count() / df.count()
df = df.dropna()

#===== STRING INDEX
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler

#colunas = list(set(df.columns)) 
indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(df) for column in ['Sex', 'Embarked', 'SibSp']]

pipeline = Pipeline(stages=indexers)
df = pipeline.fit(df).transform(df)

df = df.drop('Sex', 'Ticket', 'Embarked', 'SibSp', 'Name')
df.show()

#======== ONE HOT ENCODER
encoder = OneHotEncoderEstimator(inputCols=["Embarked_index", 'SibSp_index'],
                                 outputCols=["embarked_hot", 'sibsp_hot'])

model = encoder.fit(df)
encoded = model.transform(df)
encoded.show()

#======== VECTOR ASSEMBLER
assembler = VectorAssembler(
    inputCols = list(set(encoded.columns)-set('Survived')),
    outputCol = "features")

output = assembler.transform(encoded)

dataModel = output.select(['features', 'Survived'])

dataModel.show(truncate=False)


#======== SPLIT TEST AND TRAIN
seed = 42
train, test = dataModel.randomSplit([0.8, 0.2], seed)

test.count()
train.show()