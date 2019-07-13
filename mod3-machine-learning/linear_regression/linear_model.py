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
df = spark.read.csv('linear_regression/boston.csv', header=True, inferSchema=True)

# visualiza
df.show()


#======== ONE HOT ENCODER

from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler

encoder = OneHotEncoderEstimator(inputCols=["RAD"],
                                 outputCols=["rad_index"])

model = encoder.fit(df)
encoded = model.transform(df)
encoded.show()


# MEDV

#=============================#
# VISUALIZATION
#=============================#

import plotly.graph_objs as go
import plotly.offline as py
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot 
import cufflinks as cf  

df.show()
x = df['MEDV', 'CRIM'].toPandas()
x.iplot(mode='markers', filename='test')

#============================#
# PRE PROCESSING
#============================#

#================ VECTOR ASSEMBLER
from pyspark.ml.feature import VectorAssembler

colunas = list(set(encoded.columns)-set(['RAD', 'MEDV']))

vectorAssembler = VectorAssembler(inputCols = colunas, outputCol = 'features')
vdf = vectorAssembler.transform(encoded)
vdf = vdf.select(['features', 'MEDV'])
vdf.show(3)

#======== SPLIT TEST AND TRAIN
train, test = vdf.randomSplit([0.8, 0.2])

test.count()
train.show()

#================================#
# LINEAR REGRESSION MODELLING
#================================#

from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol = 'features', labelCol='MEDV')

# Fit the model
lrModel = lr.fit(train)
print("Coefficients: " + str(lrModel.coefficients))
print("Intercept: " + str(lrModel.intercept))

# model power
trainingSummary = lrModel.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

# evaluate model
from pyspark.ml.evaluation import RegressionEvaluator

predictions = lrModel.transform(test)
predictions.select("prediction","MEDV","features").show(5) # ADD ID PRA IDENTIFICAR

evaluator = RegressionEvaluator(predictionCol="prediction", \
                 labelCol="MEDV",metricName="r2")

print("R Squared (R2) on test data = %g" % evaluator.evaluate(predictions))
