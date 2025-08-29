df = spark.read.csv(
    root_path + "/machine_learning/diabetes/diabetes.csv", header=True, inferSchema=True
)

# Prepara la data y lo separa en dataset de entrenamiento y test
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=[
        "Pregnancies",
        "Glucose",
        "BloodPressure",
        "SkinThickness",
        "Insulin",
        "BMI",
        "DiabetesPedigreeFunction",
        "Age",
    ],
    outputCol="features",
)
output_data = assembler.transform(df)
final_data = output_data.select("features", "Outcome")
train, test = final_data.randomSplit([0.75, 0.25])

# Aplica Regresion Logistica y genera un modelo
from pyspark.ml.classification import LogisticRegression
models = LogisticRegression(labelCol="Outcome")
model = models.fit(train)
model.summary.predictions.describe().show()

# 
predict = model.evaluate(test)
predict.predictions.show(10)

# Evalua la eficiencia de las predicciones
from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator(
    rawPredictionCol="rawPrediction", labelCol="Outcome"
)
evaluator.evaluate(model.transform(test))
