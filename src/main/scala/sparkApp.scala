/**
  File: sparkApp.scala
  Purpose: The main application that runs the driver code.

-- To run the Spark application in the docker cluster, submit the job using these commands:

spark-submit --jars /home/java_libs/mysql-connector-j-8.0.32.jar --class sparkApp --master spark://sparkmaster320:7077 --files /home/spark_projs/spark_ml/src/main/resources/modeltraining.conf --driver-java-options -Dconfig.file=/home/spark_projs/spark_ml/src/main/resources/modeltraining.conf /home/spark_projs/spark_ml/target/scala-2.12/SparkMLExample-assembly-1.1.jar fromdatabase train
spark-submit --jars /home/java_libs/mysql-connector-j-8.0.32.jar --class sparkApp --master spark://sparkmaster320:7077 --files /home/spark_projs/spark_ml/src/main/resources/modeltraining.conf --driver-java-options -Dconfig.file=/home/spark_projs/spark_ml/src/main/resources/modeltraining.conf /home/spark_projs/spark_ml/target/scala-2.12/SparkMLExample-assembly-1.1.jar fromfile train
spark-submit --jars /home/java_libs/mysql-connector-j-8.0.32.jar --class sparkApp --master spark://sparkmaster320:7077 --files /home/spark_projs/spark_ml/src/main/resources/modeltraining.conf --driver-java-options -Dconfig.file=/home/spark_projs/spark_ml/src/main/resources/modeltraining.conf /home/spark_projs/spark_ml/target/scala-2.12/SparkMLExample-assembly-1.1.jar fromfile test

 *
 *   */


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.PipelineModel
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.ml.classification.{GBTClassificationModel, LogisticRegressionModel, RandomForestClassificationModel}
import org.apache.log4j.{Level, Logger}

import scala.annotation.tailrec

object sparkApp {

  val appName = "sparkApp"
  val conf: Config = ConfigFactory.load()
  val settings: AppSettings = new AppSettings(conf)
  val usage = """
  Usage: sparkApp [--datasource file|database] [--mode train|test]
  """

  def main(args: Array[String]) : Unit = {

    // Step 1:
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
    //Logger.getLogger("org").setLevel(Level.INFO)
    val logger: Logger = Logger.getLogger(appName)
    logger.setLevel(Level.INFO)

    logger.info(s"Started the application: $appName.")

    if(args.length==0) println(usage)
    val cmdArgOptions = nextArg(Map(), args.toList)

    // create a spark session configuration
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.default.parallelism", settings.minPartitions.toString)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("setWarnUnregisteredClasses", "true")
      //.set("spark.kryo.registrationRequired", "true") // <- disabled because of OpenHashMap errors when fitting models
      .registerKryoClasses(ModelData.prepareListOfKryoClasses()
      )

    //create a spark session from the configuration
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    if (args.length > 1){

      // list all spark config parameters for information, useful only when debugging
      logger.info(s"Started spark application with configuration:\n${getSparkConfigParams(sparkConf)}")

      // Step 1:
      logger.info(s"Reading data from file: ${settings.inputFile}")
      var inputDF = ModelData.readDataFile(spark, settings.inputFile)

      if(cmdArgOptions("datasource").equalsIgnoreCase("database")) {

        logger.info(s"Reading data from database table: ${settings.jdbcTableName}")

        ModelData.setDBConnProperties(
          settings.jdbcDriver,
          settings.jdbcUrl,
          settings.jdbcUser,
          settings.jdbcPassword
        )

        inputDF = ModelData.readFromJdbcConn(spark, tableName = settings.jdbcTableName)
      }

      ExploratoryAnalysis.plotCharts(inputDF)

      if(cmdArgOptions("mode").equalsIgnoreCase("train")) {

        logger.info("Started training model.")

        // Step 2:
        // explore the data frame:
        ExploratoryAnalysis.exploreDF(
          inputDF,
          settings.labelCol,
          settings.categoricalFeatures,
          settings.numericalFeatures
        )

        // Step 3:
        // Prepare a transformation pipeline on the data
        val transformPipeline: PipelineModel = ModelPipeline.transformData(
          inputDF,
          settings.labelCol,
          settings.categoricalFeatures,
          settings.numericalFeatures
        )
        // print out the pipeline
        ModelPipeline.listPipelineStages(transformPipeline)

        // Run the transformation pipeline on the dataset to prepare the data for model building
        val preparedDF: org.apache.spark.sql.DataFrame = transformPipeline.transform(inputDF)
        logger.info("Completed transforming data using the pipeline.")

        // split the prepared data into train-test datasets as: 90% and 10%
        val Array(trainingDF, testDF) = preparedDF.randomSplit(Array(0.9, 0.1))
        // At this point, these two datasets may be "cached" for improving Spark performance:
        trainingDF.cache()
        testDF.cache()

        // Step 4:
        // Fit multiple different models on this training data

        // Train a logistic regression model
        val lrModel: LogisticRegressionModel = ModelPipeline.fitLRModel(trainingDF, "label")
        ModelPipeline.getModelFitSummary(spark.sparkContext, lrModel).show()

        // Train a Gradient Boosted Decision Tree model
        val gbtModel:GBTClassificationModel = ModelPipeline.fitGBTRModel(trainingDF, "label")

        // Train a RandomForest Model
        val rfModel:RandomForestClassificationModel = ModelPipeline.fitRFModel(trainingDF, "label")

        // Step 5:
        // Evaluate each model's performance on test set
        // Make predictions on test data using the Transformer.transform() method.
        logger.info("Model performance evaluation for -> Logistic Regression:")
        // Note: model.transform will only use the 'features' column.
        val testResultLR = lrModel.transform(testDF)
        // custom processing to add class1 probability as a separate column for easier calculations
        val testResultLRWithProbsDF = ModelPipeline.addBinaryProbabilities(testResultLR)
        // evaluate model performance on test set
        ModelPipeline.evaluatePerformance(testResultLRWithProbsDF)

        logger.info("Model performance evaluation for -> Gradient Boosted Decision Trees Model:")
        val testResultGBDT = gbtModel.transform(testDF)
        val testResultGBDTWithProbsDF = ModelPipeline.addBinaryProbabilities(testResultGBDT)
        ModelPipeline.evaluatePerformance(testResultGBDTWithProbsDF)

        logger.info("Model performance evaluation for -> Random Forest Model:")
        val testResultRF = rfModel.transform(testDF)
        val testResultRFWithProbsDF = ModelPipeline.addBinaryProbabilities(testResultRF)
        ModelPipeline.evaluatePerformance(testResultRFWithProbsDF)

        // Step 6:
        logger.info("Now writing fitted LR model to disk at: " + settings.lrModelSavePath)
        lrModel.write.overwrite().save(settings.lrModelSavePath)

        logger.info("Now writing fitted GBDT model to disk at: " + settings.gbdtModelSavePath)
        gbtModel.write.overwrite().save(settings.gbdtModelSavePath)

        logger.info("Now saving the transformation pipeline to disk at: " + settings.transformPipelineSavePath)
        transformPipeline.write.overwrite().save(settings.transformPipelineSavePath)

      }else{
        // Model Prediction/inference:
        // Alternatively, load an already saved Pipeline from disk:
        val transformPipeline: PipelineModel = PipelineModel.load(settings.transformPipelineSavePath)

        // Run the transformation pipeline on the dataset to prepare the data for model building
        val preparedDF: org.apache.spark.sql.DataFrame = transformPipeline.transform(inputDF)
        logger.info("Completed transforming data using the transformation pipeline.")

        // Alternatively, load an already fitted model from disk:
        val lrModel = LogisticRegressionModel.load(settings.lrModelSavePath)

        logger.info("Generating inferences for -> Logistic Regression:")
        val testResultLR = lrModel.transform(preparedDF)

        // custom processing to add class1 probability as a separate column for easier calculations
        val testResultLRWithProbsDF = ModelPipeline.addBinaryProbabilities(testResultLR)

        //write results to a csv file:
        ModelData.saveDataFrameToCSV(testResultLRWithProbsDF, settings.outputFile)
      }
    }
    else{
      logger.error("No arguments given.\nUsage:")
      logger.error("First argument should be either - fromfile or fromdatabase")
      logger.error("Second argument should be either - train or test")
    }

    spark.stop()
  }

  /**
   * Print all of Spark's configuration parameters
   * @param config: SparkContext of the current spark session
   */
  private def getSparkConfigParams(config: SparkConf): String = {
    val strBuilder = new StringBuilder()
    // for each key-value pair, prepare a string key=value, append it to the string builder
    for ((k:String, v:String) <- config.getAll) {strBuilder ++= s"\n$k = $v"}
    // return prepared string
    strBuilder.mkString
  }

  /**
   * Parse the next command line argument, recursively building up the map.
   * @param map The hashmap in which all switches and their values are stored as key-value pairs
   * @param list The command line arguments passed as a list
   * @return
   */
  @tailrec
  private def nextArg(map: Map[String, String], list: List[String]): Map[String, String] = {
    list match {
      case Nil => map
      case "--datasource" :: value :: tail =>
        nextArg(map ++ Map("datasource" -> value.toLowerCase()), tail)
      case "--mode" :: value :: tail =>
        nextArg(map ++ Map("mode" -> value.toLowerCase()), tail)
      case unknown :: _ =>
        println("Unknown option " + unknown)
        map
    }
  }
}

