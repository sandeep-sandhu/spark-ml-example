/**
 * File: ModelPipeline.scala
 * Purpose: Train and infer using model pipelines
 *
 */

import org.apache.logging.log4j.scala.{Logger, Logging}
import org.apache.logging.log4j.Level

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions.{col, udf, _}

import org.apache.spark.ml.classification.{GBTClassifier, LogisticRegression, LogisticRegressionModel, RandomForestClassifier}
import org.apache.spark.ml.{Model, Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg

import sparkApp.appName


object ModelPipeline extends org.apache.logging.log4j.scala.Logging{

  val spark: SparkSession = SparkSession.active
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /**
   * Transforms the raw dataset columns to a form usable for training the models -
   * e.g. string to categorical variables, one-hot encoding, scaling of continuous variables, etc.
   *
   * @param inputDF Input raw dataset
   * @param labelColname Name of the label column
   * @param categoricalFeatures List of column names which are categorical features
   * @param numericalFeatures List of column names which are numerical features
   * @return The fitted transformation pipeline
   */
  def transformData(inputDF: Dataset[ModelDataRecord],
                    labelColname: String,
                    categoricalFeatures: Array[String],
                    numericalFeatures: Array[String]): PipelineModel = {

    // this buffer "xforms" will accumulate all our transformations till we're ready to put them in a pipeline
    var xforms = scala.collection.mutable.ArrayBuffer.empty[PipelineStage];

    // first of all, index the binary label column:
    val labelIndexer = new StringIndexer()
      .setInputCol(labelColname)
      .setOutputCol("label")
    xforms += labelIndexer;

    // add a column indexer for each categorical column:
    categoricalFeatures.foreach(x => xforms += new StringIndexer().setInputCol(x).setOutputCol("idx_" + x))
    logger.info("Indexing categorical variables.")

    categoricalFeatures.foreach(x => xforms += new OneHotEncoder().setInputCol("idx_" + x).setOutputCol("vec_idx_" + x))
    logger.info("On-hot encoding all categorical variables.")

    // gather all column names, these will be used in vector assembler later:
    var allColNames = scala.collection.mutable.ArrayBuffer.empty[String]
    categoricalFeatures.foreach(x => allColNames += "vec_idx_%s".format(x))

    // gather all numerical variables to assemble into a vector for scaling
    var numericalColNames = scala.collection.mutable.ArrayBuffer.empty[String]
    numericalFeatures.foreach(y => numericalColNames += y)
    val assembler1 = new VectorAssembler()
      .setInputCols(numericalColNames.toArray)
      .setOutputCol("numericalfeatures")
    xforms += assembler1;
    logger.info("Assembled together all numerical variables.")

    // apply a min-max scaler for the numerical features:
    xforms += new MinMaxScaler().setInputCol("numericalfeatures").setOutputCol("scaledfeatures");
    allColNames += "scaledfeatures"
    logger.info("Scaled all numerical variables by min-max scaler.")

    // finally, collect all columns into the "features" column, this is a vector object
    val assembler2 = new VectorAssembler()
      .setInputCols(allColNames.toArray)
      .setOutputCol("features")
    xforms += assembler2;

    logger.info("Assembling pipeline with the following transformations: \n" + xforms.mkString(" \n"))
    val xformPipeline = new Pipeline()
      .setStages(xforms.toArray);

    val xformFitted = xformPipeline.fit(inputDF);
    logger.info("Completed fitting the pipeline")

    return xformFitted
  }


  /**
   * Train a logistic regression model
   *
   * @param training     The input dataset for training
   * @param labelColName The name of the label column
   * @return The fitted logistic regression model selected by cross validation
   */
  def fitLRModel(training: org.apache.spark.sql.DataFrame,
                 labelColName: String): CrossValidatorModel = {
    logger.info("Training a logistic regression model.")

    // Create a LogisticRegression instance. This instance is an Estimator.
    val lr = new LogisticRegression()

    // We may set parameters using setter methods.
    lr.setMaxIter(100)
      .setRegParam(0.0025)
      .setElasticNetParam(0.0075)
      .setFamily("binomial")
      .setFitIntercept(true)
      .setThreshold(0.35)
      .setLabelCol(labelColName);

    // Print out the parameters, documentation, and any default values.
    logger.info(s"---Logistic Regression parameters for training:---\n ${lr.explainParams()}\n")
    /*
    aggregationDepth: suggested depth for treeAggregate (>= 2) (default: 2)
    elasticNetParam: the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty (default: 0.0, current: 0.005)
    family: The name of family which is a description of the label distribution to be used in the model. Supported options: auto, binomial, multinomial. (default: auto, current: binomial)
    featuresCol: features column name (default: features)
    fitIntercept: whether to fit an intercept term (default: true, current: true)
    labelCol: label column name (default: label, current: label)
    lowerBoundsOnCoefficients: The lower bounds on coefficients if fitting under bound constrained optimization. (undefined)
    lowerBoundsOnIntercepts: The lower bounds on intercepts if fitting under bound constrained optimization. (undefined)
    maxBlockSizeInMB: Maximum memory in MB for stacking input data into blocks. Data is stacked within partitions. If more than remaining data size in a partition then it is adjusted to the data size. Default 0.0 represents choosing optimal value, depends on specific algorithm. Must be >= 0. (default: 0.0)
    maxIter: maximum number of iterations (>= 0) (default: 100, current: 100)
    predictionCol: prediction column name (default: prediction)
    probabilityCol: Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities (default: probability)
    rawPredictionCol: raw prediction (a.k.a. confidence) column name (default: rawPrediction)
    regParam: regularization parameter (>= 0) (default: 0.0, current: 0.0025)
    standardization: whether to standardize the training features before fitting the model (default: true)
    threshold: threshold in binary classification prediction, in range [0, 1] (default: 0.5, current: 0.4)
    thresholds: Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold (undefined)
    tol: the convergence tolerance for iterative algorithms (>= 0) (default: 1.0E-6)
    upperBoundsOnCoefficients: The upper bounds on coefficients if fitting under bound constrained optimization. (undefined)
    upperBoundsOnIntercepts: The upper bounds on intercepts if fitting under bound constrained optimization. (undefined)
    weightCol: weight column name. If this is not set or empty, we treat all instance weights as 1.0 (undefined)
    */

    // Now Learn a LogisticRegression model. This uses the parameters stored in lr.
    //val model1 = lr.fit(training); // <-- commented out since param grid is used

    // We use a ParamGridBuilder to construct a grid of hyper-parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.0025))
      //.addGrid(lr.fitIntercept)
      //.addGrid(lr.standardization)
      .addGrid(lr.elasticNetParam, Array(0.0075))
      .addGrid(lr.threshold, Array(0.35))
      .build()

    val binaryEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol(labelColName)
      .setMetricName("areaUnderPR");

    val xfoldValidator = new CrossValidator()
      .setEstimator(lr)
      .setNumFolds(10)
      .setEvaluator(binaryEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setCollectSubModels(false)
      .setParallelism(2)

    // Run train validation split, and choose the best set of parameters.
    val model1 = xfoldValidator.fit(training)

    // we can view the parameters it used during fit().
    // This prints the parameter (name: value) pairs, where names are unique IDs for this instance.
    val fittedParamMap = model1.bestModel.parent.extractParamMap
    logger.info(s"---Cross-fold validated Logistic Regression Model was fit using parameters:---${fittedParamMap}")

    return model1
  }

  /**
   * Train a Gradient Boosted Trees model.
   * @param training Training dataset
   * @param labelColName Name of th label column
   * @return Fitted GBDTree model
   */
  def fitGBTRModel( training: org.apache.spark.sql.DataFrame,
                    labelColName: String): CrossValidatorModel = {

    logger.info("Training a Gradient Boosted Decision Trees model.")

    // Train a GBT model, set the hyper-parameters:
    val gbt = new GBTClassifier()
      .setLabelCol(labelColName)
      .setFeaturesCol("features")
      .setMaxDepth(4)
      .setMaxIter(10)
      .setMinInstancesPerNode(8)
      .setStepSize(0.4)
      .setMaxBins(32)
      .setFeatureSubsetStrategy("auto")

    // Print out the parameters, documentation, and any default values.
    logger.info(s"---Gradient Boosted Decision Trees parameters for training:---\n ${gbt.explainParams()}\n")
    /*
    cacheNodeIds: If false, the algorithm will pass trees to executors to match instances with nodes. If true, the algorithm will cache node IDs for each instance. Caching can speed up training of deeper trees. (default: false)
    checkpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext (default: 10)
    featureSubsetStrategy: The number of features to consider for splits at each tree node. Supported options: auto, all, onethird, sqrt, log2, (0.0-1.0], [1-n]. (default: all, current: auto)
    featuresCol: features column name (default: features, current: features)
    impurity: Criterion used for information gain calculation (case-insensitive). Supported options: variance (default: variance)
    labelCol: label column name (default: label, current: label)
    leafCol: Leaf indices column name. Predicted leaf index of each instance in each tree by preorder (default: )
    lossType: Loss function which GBT tries to minimize (case-insensitive). Supported options: logistic (default: logistic)
    maxBins: Max number of bins for discretizing continuous features.  Must be at least 2 and at least number of categories for any categorical feature. (default: 32)
    maxDepth: Maximum depth of the tree. (Nonnegative) E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. Must be in range [0, 30]. (default: 5)
    maxIter: maximum number of iterations (>= 0) (default: 20, current: 10)
    maxMemoryInMB: Maximum memory in MB allocated to histogram aggregation. (default: 256)
    minInfoGain: Minimum information gain for a split to be considered at a tree node. (default: 0.0)
    minInstancesPerNode: Minimum number of instances each child must have after split.  If a split causes the left or right child to have fewer than minInstancesPerNode, the split will be discarded as invalid. Must be at least 1. (default: 1)
    minWeightFractionPerNode: Minimum fraction of the weighted sample count that each child must have after split. If a split causes the fraction of the total weight in the left or right child to be less than minWeightFractionPerNode, the split will be discarded as invalid. Should be in interval [0.0, 0.5) (default: 0.0)
    predictionCol: prediction column name (default: prediction)
    probabilityCol: Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities (default: probability)
    rawPredictionCol: raw prediction (a.k.a. confidence) column name (default: rawPrediction)
    seed: random seed (default: -1287390502)
    stepSize: Step size (a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of each estimator. (default: 0.1)
    subsamplingRate: Fraction of the training data used for learning each decision tree, in range (0, 1]. (default: 1.0)
    thresholds: Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold (undefined)
    validationIndicatorCol: name of the column that indicates whether each row is for training or for validation. False indicates training; true indicates validation. (undefined)
    validationTol: Threshold for stopping early when fit with validation is used.If the error rate on the validation input changes by less than the validationTol,then learning will stop early (before `maxIter`).This parameter is ignored when fit without validation is used. (default: 0.01)
    weightCol: weight column name. If this is not set or empty, we treat all instance weights as 1.0 (undefined)
    */
    //val gbtModel = gbt.fit(training) // <-- commented out since we're using cross-validator

    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.minInstancesPerNode, Array(8))
      .addGrid(gbt.maxDepth, Array(4))
      .addGrid(gbt.stepSize, Array(0.4))
      .build()

    val binaryEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol(labelColName)
      .setMetricName("areaUnderPR");

    val xfoldValidator = new CrossValidator()
      .setEstimator(gbt)
      .setNumFolds(10)
      .setEvaluator(binaryEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setCollectSubModels(false)
      .setParallelism(2)

    // Run train validation split, and choose the best set of parameters.
    val model1 = xfoldValidator.fit(training)

    // This prints the parameter (name: value) pairs, where names are unique IDs for this instance.
    val fittedParamMap = model1.bestModel.parent.extractParamMap
    logger.info(s"---Cross-fold validated Gradient Boosted Decision Trees Model was fit using parameters:---${fittedParamMap}")

    return model1
  }

  /**
   * Train a RandomForest model
   * @param training Training dataset
   * @param labelColName Column name for the label
   * @return Best performing trained model after cross validation
   */
  def fitRFModel( training: org.apache.spark.sql.DataFrame,
                  labelColName: String): CrossValidatorModel = {
    logger.info("Training a RandomForest model.")

    // Train a RandomForest model, set the hyper-parameters:
    val rf = new RandomForestClassifier()
      .setLabelCol(labelColName)
      .setFeaturesCol("features")
      .setMaxBins(32)
      .setMaxDepth(12)
      .setNumTrees(150)

    // Print out the parameters, documentation, and any default values.
    logger.info(s"---RandomForest parameters for training:---\n ${rf.explainParams()}\n")
    /*
    bootstrap: Whether bootstrap samples are used when building trees. (default: true)
    cacheNodeIds: If false, the algorithm will pass trees to executors to match instances with nodes. If true, the algorithm will cache node IDs for each instance. Caching can speed up training of deeper trees. (default: false)
    checkpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext (default: 10)
    featureSubsetStrategy: The number of features to consider for splits at each tree node. Supported options: auto, all, onethird, sqrt, log2, (0.0-1.0], [1-n]. (default: auto)
    featuresCol: features column name (default: features, current: features)
    impurity: Criterion used for information gain calculation (case-insensitive). Supported options: entropy, gini (default: gini)
    labelCol: label column name (default: label, current: label)
    leafCol: Leaf indices column name. Predicted leaf index of each instance in each tree by preorder (default: )
    maxBins: Max number of bins for discretizing continuous features.  Must be at least 2 and at least number of categories for any categorical feature. (default: 32)
    maxDepth: Maximum depth of the tree. (Nonnegative) E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. Must be in range [0, 30]. (default: 5)
    maxMemoryInMB: Maximum memory in MB allocated to histogram aggregation. (default: 256)
    minInfoGain: Minimum information gain for a split to be considered at a tree node. (default: 0.0)
    minInstancesPerNode: Minimum number of instances each child must have after split.  If a split causes the left or right child to have fewer than minInstancesPerNode, the split will be discarded as invalid. Must be at least 1. (default: 1)
    minWeightFractionPerNode: Minimum fraction of the weighted sample count that each child must have after split. If a split causes the fraction of the total weight in the left or right child to be less than minWeightFractionPerNode, the split will be discarded as invalid. Should be in interval [0.0, 0.5) (default: 0.0)
    numTrees: Number of trees to train (at least 1) (default: 20, current: 50)
    predictionCol: prediction column name (default: prediction)
    probabilityCol: Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities (default: probability)
    rawPredictionCol: raw prediction (a.k.a. confidence) column name (default: rawPrediction)
    seed: random seed (default: 207336481)
    subsamplingRate: Fraction of the training data used for learning each decision tree, in range (0, 1]. (default: 1.0)
    thresholds: Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold (undefined)
    weightCol: weight column name. If this is not set or empty, we treat all instance weights as 1.0 (undefined)
    */

    //val rfModel = rf.fit(training) // <-- commented out since we're using cross-validator

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(150))
      .addGrid(rf.maxDepth, Array(12))
      .addGrid(rf.minInstancesPerNode, Array(1,2,5))
      .build()

    val binaryEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol(labelColName)
      .setMetricName("areaUnderPR");

    val xfoldValidator = new CrossValidator()
      .setEstimator(rf)
      .setNumFolds(10)
      .setEvaluator(binaryEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setCollectSubModels(false)
      .setParallelism(2)

    // Run train validation split, and choose the best set of parameters.
    val model1 = xfoldValidator.fit(training)

    // This prints the parameter (name: value) pairs, where names are unique IDs for this instance.
    val fittedParamMap = model1.bestModel.parent.extractParamMap
    logger.info(s"---Cross-fold validated RandomForest Model was fit using parameters:---${fittedParamMap}")

    return model1
  }

  /**
   * Evaluate Model's performance on test dataset.
   * It calculates the area under ROC, area under P-R curve, Log scoring rule, and the contingency table.
   * @param testResultDF Predictions dataframe with predicted probabilities and actual outcomes for comparison
   * @param labelColName Actual outcome (label)
   * @param predictedLabelColName Column name with the predicted label
   * @param class1ProbColName Column name with the predicted class1 probability
   */
  def evaluatePerformance(testResultDF: DataFrame,
                          labelColName: String = "label",
                          predictedLabelColName: String = "prediction",
                          class1ProbColName: String = "p1"): Unit = {

    val binEvalPR = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderPR").setLabelCol(labelColName)
      .evaluate(testResultDF);
    logger.info("For test set, Area under Precision-Recall curve is: " + binEvalPR.doubleValue());

    val binEvalROC = new BinaryClassificationEvaluator()
      .setLabelCol(labelColName)
      .evaluate(testResultDF);
    logger.info("For test set, Area under ROC curve is: " + binEvalROC.doubleValue());

    val loglossScore: Double = logScoringMetric(testResultDF,
      labelCol = labelColName,
      predictProb = class1ProbColName)
    logger.info("For test set, the log-loss score is: " + loglossScore)

    println("For test set, the contingency table is: ")
    val contTabDF = testResultDF.stat.crosstab(predictedLabelColName, labelColName)
    contTabDF.show()
    val confusionMat = contTabDF.collect()

    logger.info("For test set, the False Positives = %s".format( confusionMat(0)(1) ))
    logger.info("For test set, the True Positives = %s".format( confusionMat(0)(2) ))
    logger.info("For test set, the True Negatives = %s".format( confusionMat(1)(1) ))
    logger.info("For test set, the False Negatives = %s".format( confusionMat(1)(2) ))

  }

  /**
   * Split probability vector column into individual binary class probability columns - p1 and p0
   * @param inputDF Input dataframe with model predictions
   * @param probCol Name of the probability column, default is "probability"
   * @return Dataframe with additional column for class 1 probability
   */
  def addBinaryProbabilities(inputDF: org.apache.spark.sql.DataFrame, probCol:String = "probability"): DataFrame = {

    // Breakup vector field "probability" into prob of class "1":
    // Create a UDF to convert VectorUDT to ArrayType
    val vecToArray = udf((xs: linalg.Vector) => xs.toArray)
    // Add a ArrayType Column: PredictProbabArr
    val dfProbArr = inputDF.withColumn("PredictProbabArr", vecToArray($"probability"))
    // Array of element names that need to be fetched:
    val elements = Array("p0", "p1")
    // Create a SQL-like expression using the array
    val sqlExpr = elements.zipWithIndex.map { case (alias, idx) => col("PredictProbabArr").getItem(idx).as(alias) }
    //add the columns to the dataframe
    val testResultWithProbsDF = dfProbArr.select((col("*") +: sqlExpr): _*)
      .drop(colNames = "PredictProbabArr", "p0")

    return testResultWithProbsDF
  }

   /**
   * Calculates the log-loss using the log-scoring rule
    * In the formula here: "label"=y and "p1"=p
   * logloss = -y.log(p)  +  log(1 -p).(1 - y)
   *
   * @param inputDF Prediction results with actual and prediction probability
   * @param predictProb Column name to be used for predicted probability of class 1
   * @param labelCol Column name to be used for actual value
   * @return The log loss calculated as per the log-scoring rule
   */
  def logScoringMetric(inputDF: DataFrame, predictProb:String="p1", labelCol:String = "label"): Double = {

    val testResultsLoglossDF = inputDF.withColumn(
      "loglossT1",
      col("label") * org.apache.spark.sql.functions.log(col("p1")) * -1.0
    ).withColumn(
      colName = "loglossT2",
      org.apache.spark.sql.functions.expr("1 - p1")
    ).withColumn(
      "logloss",
      col("loglossT1") + org.apache.spark.sql.functions.log(col("loglossT2")) * expr("1 - label")
    ).drop(colNames = "loglossT1", "loglossT2")

    val logloss: Any = testResultsLoglossDF.select(avg("logloss")).collect()(0)(0);

    return logloss.asInstanceOf[Double]
  }

  /**
   * Utility function to prints the predictions dataframe.
   * Caution: This collects all the data to the driver.
   * @param testResultDF Predictions data frame
   */
  def printPredictionsDF(testResultDF: DataFrame): Unit = {
    // Coalesce dataframe to one partition, i.e. to driver
    val testResults = testResultDF.coalesce(1)

    println("Test results head: " + testResults.head)

    println("Test set with predictions:");
    //testResults.foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
    //  println(s"($features, $label) -> prob=$prob, prediction=$prediction")
    //}
  }
  
}