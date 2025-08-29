df = spark.read.csv(root_path + "/machine_learning/diabetes/diabetes.csv", header=True,inferSchema=True)

from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=['Pregnancies','Glucose','BloodPressure','SkinThickness','Insulin','BMI','DiabetesPedigreeFunction','Age'],outputCol='features')
output_data = assembler.transform(df)

from pyspark.ml.classification import LogisticRegression
final_data = output_data.select('features','Outcome')

train , test = final_data.randomSplit([0.7,0.3])
models = LogisticRegression(labelCol='Outcome')
model = models.fit(train)

summary = model.summary

summary.predictions.describe().show()

from pyspark.ml.evaluation import BinaryClassificationEvaluator
predictions = model.evaluate(test)

evaluator = BinaryClassificationEvaluator(rawPredictionCol='rawPrediction', labelCol='Outcome')
evaluator.evaluate(model.transform(test))

