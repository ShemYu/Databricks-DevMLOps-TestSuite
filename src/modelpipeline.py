# Databricks notebook source
import mlflow

# %run /Users/sandy1990418@gmail.com/datapipeline
feature_table_name = "databricks_test_mr_df"
project_name = "SandyMRRandomForestCalssifier"
exp_name = f"/Users/shauns4y@gmail.com/{project_name}"
tunning_step_run = "tunning"
model_name = "testing_model"

# COMMAND ----------

try:
    exp_id = mlflow.create_experiment(exp_name)
except Exception as e:
    exp_id = mlflow.get_experiment_by_name(exp_name).experiment_id
exp_id

# COMMAND ----------

exp_info = mlflow.set_experiment(experiment_id=exp_id)
exp_info

# COMMAND ----------

df = spark.sql(f"SELECT * FROM {feature_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### data split to train and test

# COMMAND ----------

import pyspark.sql.functions as F
from databricks.feature_store import FeatureStoreClient, feature_table
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                   RegressionEvaluator)
from pyspark.ml.feature import StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql.functions import when

#########################################################################
############################# load data  ################################
#########################################################################
df = df.withColumn("gender", F.expr("case when(gender='F') then 2 when(gender='M') then 1 else 0 end"))
df = df.withColumn("lag_sum_total", when(df.lag_sum_total == 0,0).otherwise(1))

train_df, test_df = df.randomSplit([.7, .3], seed=42)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### data processing pipeline

# COMMAND ----------


#########################################################################
################## to check data processing pipeline ####################
#########################################################################

## get categorical columns 
categorical_cols = ['gender','lag_sum_total']
## create new col named as col_name_Index
index_output_cols = [x + "Index" for x in categorical_cols]

## convert to stirng indexer 
string_indexer = StringIndexer(inputCols=categorical_cols, outputCols=index_output_cols, handleInvalid="skip")

## get numerical columns 
numeric_cols = [field for (field, dataType) in train_df.dtypes if ((dataType == "bigint") |(dataType == "int")& (field != "lag_sum_total")& (field != "gender")& (field != "yyyymm"))]

## combine numerical cols to vector 
vec_numerical = VectorAssembler(inputCols=numeric_cols, outputCol="numerical_features",handleInvalid='skip')

## standards 
normalizer = StandardScaler(inputCol="numerical_features", outputCol="scaledFeatures", withStd=True, withMean=True)
## using pipeline to execute normalization
# pipeline_normalize = Pipeline(stages=[vec_numerical,normalizer])

## combine categorical and numerical features as model inputs 
assembler_inputs = ['genderIndex'] + ["scaledFeatures"]
vec_assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### single model training 

# COMMAND ----------


#########################################################################
############################ prepare model ##############################
#########################################################################

# build model 
# rf = RandomForestClassifier(labelCol="lag_sum_totalIndex", maxBins=40) #
rf = RandomForestClassifier(featuresCol='features', labelCol='lag_sum_totalIndex')
datapipeline=Pipeline(stages=[string_indexer,vec_numerical,normalizer,vec_assembler])


datapipeline = datapipeline.fit(train_df)
train_df = datapipeline.transform(train_df)


import mlflow

## autolog model information 
mlflow.pyspark.ml.autolog(log_models=True)
model=rf.fit(train_df)


# COMMAND ----------


import mlflow

## autolog model information 
mlflow.pyspark.ml.autolog(log_models=True)
model=rf.fit(train_df)


# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import pyspark.sql.functions as F
import seaborn as sns
from pyspark.ml import Pipeline
from pyspark.ml.classification import (LogisticRegression, NaiveBayes,
                                       RandomForestClassifier)
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                   MulticlassClassificationEvaluator)
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, explode, lit
from pyspark.sql.types import FloatType

test_df = datapipeline.transform(test_df)
pred_df = model.transform(test_df)


def confusion_matrix(pred_df):
    preds_labels = pred_df.select(['prediction','lag_sum_totalIndex']).withColumn('lag_sum_total', F.col('lag_sum_totalIndex').cast(FloatType())).orderBy('prediction')
    preds_labels = preds_labels.select(['prediction','lag_sum_totalIndex'])
    metrics = MulticlassMetrics(preds_labels.rdd.map(tuple))
    return metrics.confusionMatrix().toArray()

def confusion_matrix_plot(conf_mat, ax, title = 'Confusion Matrix'):
    names = ['True Negative','False Positive','False Negative','True Positive']
    number = ["{0:0.0f}".format(value) for value in conf_mat.flatten()]
    percent = ["{0:.2%}".format(value) for value in conf_mat.flatten()/np.sum(conf_mat)]
    labels = [f"{v1}\n\n{v2}\n\n{v3}" for v1, v2, v3 in zip(names, number, percent)]
    labels = np.asarray(labels).reshape(2,2)
    sns.heatmap(conf_mat, annot=labels, fmt='', cmap='Blues', cbar=True)
 
    return None

conf_rfc = confusion_matrix(pred_df)
confusion_matrix_plot(conf_rfc,'Random Forest Classifier - Confusion Matrix')

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(5,5))
plt.plot([0, 1], [0, 1], 'r--')

plt.plot(model.summary.roc.select('FPR').collect(),
         model.summary.roc.select('TPR').collect())
# cv_best_model_roc = pipeline_model.bestModel.stages[-1].summary.roc.toPandas()
# plt.plot(cv_best_model_roc['FPR'],cv_best_model_roc['TPR'])
plt.ylabel('False Positive Rate')
plt.xlabel('True Positive Rate')
plt.title('ROC Curve')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### hyperopt

# COMMAND ----------

from hyperopt import hp

######################################################################################
############################ setting objective function ##############################
######################################################################################
bcEvaluator =  BinaryClassificationEvaluator(labelCol='lag_sum_totalIndex', rawPredictionCol='prediction',metricName="areaUnderROC")
modelpipeline=Pipeline(stages=[rf])


def objective_function(params):    
    mlflow.pyspark.ml.autolog(log_models=True)
    # set the hyperparameters that we want to tune
    # min_ins = int(params['minInstancesPerNode'])
    # max_bins = int(params['maxBins'])
    max_depth = params["max_depth"]
    num_trees = params["num_trees"]
    ## need nested = True
    with mlflow.start_run(nested=True):
        estimator = modelpipeline.copy({rf.maxDepth: max_depth, rf.numTrees: num_trees})
        model = estimator.fit(train_df)

        preds = model.transform(test_df)
        ## evaluate index
        auc = -1*bcEvaluator.evaluate(preds)
        ## log to experiment
        mlflow.log_metric("negative_auc", auc)
        # mlflow.log_metric('negative_auc',auc)

    return auc



######################################################################################
################################ define search scope #################################
######################################################################################


## search space(define range of hyparameter )
search_space = {
    # 'minInstancesPerNode': hp.uniform('minInstancesPerNode', 10, 15),
    # 'maxBins': hp.uniform('maxBins', 2, 6),
    "max_depth": hp.quniform("max_depth", 2, 5, 1),
    "num_trees": hp.quniform("num_trees", 10, 100, 1)
}

######################################################################################
################################ setting evaluator ###################################
######################################################################################

import mlflow
import mlflow.spark
import numpy as np
from hyperopt import Trials, fmin, tpe

# tpe.suggest = search algorithm
num_evals = 4
trials = Trials()
with mlflow.start_run(run_name=tunning_step_run):
    mlflow.pyspark.ml.autolog(log_models=True)
    best_hyperparam = fmin(fn=objective_function, 
                        space=search_space,
                        algo=tpe.suggest, 
                        max_evals=num_evals,
                        trials=trials,
                        rstate=np.random.default_rng(42))
mlflow.end_run()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### model evaluate 

# COMMAND ----------

import mlflow


tunning_step_run = "tunning"
runs = mlflow.search_runs(exp_id, filter_string=f"tags.mlflow.runName='{tunning_step_run}'")
runs.run_id

# COMMAND ----------

# runs = mlflow.search_runs(filter_string=f"tags.mlflow.parentRunId='{runs.run_id[0]}'")
run_id = "5cfe23b617184cb2afd283b661870379"
runs = mlflow.search_runs(filter_string=f"tags.mlflow.parentRunId='{run_id}'")
runs

# COMMAND ----------

test_df.show(1)

# COMMAND ----------

for idx, run in runs.iterrows():
    model_uri = f"runs:/{run.run_id}/model"
    result = mlflow.evaluate(
        model_uri,
        test_df,
        targets='lag_sum_totalIndex',
        model_type="classifier",
        evaluators=["default"],
    )

# COMMAND ----------

runs = mlflow.search_runs(filter_string=f"tags.mlflow.parentRunId='{run_id}'", order_by=["metrics.areaUnderROC_unknown_dataset DESC"])
runs


# COMMAND ----------

runs.iloc[0]

# COMMAND ----------

mlflow.register_model(f"runs:/{runs.iloc[0].run_id}/model", "BestModel")
