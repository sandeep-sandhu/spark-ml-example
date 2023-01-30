/**
 *  File: sparkApp.scala

cd /d %USERPROFILE%\Documents\src\spark_projs\spark_ml

sbt package

-- local spark submit:
spark-submit --class sparkApp --name SparkDemo --jars c:\bin\lib\mysql-connector-j-8.0.32.jar target\scala-2.12\sparkmlexample_2.12-1.1.jar fromdatabase
spark-submit --master local[*] --class sparkApp --name SparkDemo --files C:\Users\08647W744\Documents\src\spark_projs\spark_ml\src\main\resources\modeltraining.conf --driver-java-options -Dconfig.file=C:/Users/08647W744/Documents/src/spark_projs/spark_ml/src/main/resources/modeltraining.conf target\scala-2.12\SparkMLExample-assembly-1.1.jar fromdatabase train
spark-submit --master local[*] --class sparkApp --name SparkDemo --files C:\Users\08647W744\Documents\src\spark_projs\spark_ml\src\main\resources\modeltraining.conf --driver-java-options -Dconfig.file=C:/Users/08647W744/Documents/src/spark_projs/spark_ml/src/main/resources/modeltraining.conf target\scala-2.12\SparkMLExample-assembly-1.1.jar fromfile train


spark-submit --master local[*] --class sparkApp --name SparkDemo --files C:\Users\08647W744\Documents\src\spark_projs\spark_ml\src\main\resources\modeltraining.conf,C:\Users\08647W744\Documents\src\spark_projs\spark_ml\src\main\resources\log4j2.properties --driver-java-options "-Dconfig.file=C:/Users/08647W744/Documents/src/spark_projs/spark_ml/src/main/resources/modeltraining.conf -Dlog4j.configuration=file:/C:/Users/08647W744/Documents/src/spark_projs/spark_ml/src/main/resources/log4j2.properties " target\scala-2.12\SparkMLExample-assembly-1.1.jar fromfile train

spark-submit --master local[*] --class sparkApp --name SparkDemo --files C:\Users\08647W744\Documents\src\spark_projs\spark_ml\src\main\resources\modeltraining.conf,C:\Users\08647W744\Documents\src\spark_projs\spark_ml\src\main\resources\log4j2.properties --driver-java-options "-Dconfig.file=C:/Users/08647W744/Documents/src/spark_projs/spark_ml/src/main/resources/modeltraining.conf -Dlog4j2.configurationFile=file:/C:/Users/08647W744/Documents/src/spark_projs/spark_ml/src/main/resources/log4j2.properties " target\scala-2.12\SparkMLExample-assembly-1.1.jar fromfile test

-- Spark cluster in docker:
/opt/bitnami/spark/spark-submit --jars /home/java_libs/mysql-connector-j-8.0.32.jar --class sparkApp --master spark://sparkmaster320:7077 /home/spark_projs/spark_ml/target/scala-2.12/sparkmlexample_2.12-1.1.jar fromdatabase train
/opt/bitnami/spark/bin/spark-submit --jars /home/java_libs/mysql-connector-j-8.0.32.jar --class sparkApp --master spark://sparkmaster320:7077 --files /home/spark_projs/spark_ml/src/main/resources/modeltraining.conf --driver-java-options -Dconfig.file=/home/spark_projs/spark_ml/src/main/resources/modeltraining.conf /home/spark_projs/spark_ml/target/scala-2.12/SparkMLExample-assembly-1.1.jar fromdatabase train

 *  
 *   */


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.PipelineModel
import org.apache.spark.util.collection.OpenHashMap

import org.apache.logging.log4j.{Level, LogManager}
import com.typesafe.config.{Config, ConfigFactory}
import java.lang.annotation.Target

object sparkApp extends org.apache.logging.log4j.scala.Logging{

  val appName = "sparkApp"
  val conf: Config = ConfigFactory.load()
  val settings: AppSettings = new AppSettings(conf)

  def main(args: Array[String]) : Unit = {

    // Step 1:
    logger.info("Started the application, now creating spark session.")
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("setWarnUnregisteredClasses", "true")
      //.set("spark.kryo.registrationRequired", "true")
      .registerKryoClasses(ModelData.prepareListOfKryoClasses()
      )

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    if (args.length > 1){
      val dataSourceChoice = args(0);
      val trainOrPredict = args(1);

      // Step 1:
      logger.info(s"Reading data from file: ${settings.inputFile}")
      var inputDF = ModelData.readDataFile(spark, settings.inputFile);

      if(dataSourceChoice.equalsIgnoreCase("fromdatabase")) {
        logger.info(s"Reading data from database table: ${settings.jdbcTableName}")
        ModelData.setDBConnProperties(
          settings.jdbcDriver,
          settings.jdbcUrl,
          settings.jdbcUser,
          settings.jdbcPassword
        )
        inputDF = ModelData.readFromJdbcConn(spark, tableName = settings.jdbcTableName)
      }

      if(trainOrPredict.equalsIgnoreCase("train")) {
        // Step 2:
        // explore the data frame:
        ExploratoryAnalysis.exploreDF(
          inputDF,
          settings.labelCol,
          settings.categoricalFeatures,
          settings.numericalFeatures
        );

        // Step 3:
        // Prepare a transformation pipeline on the data
        val transformPipeline: PipelineModel = ModelPipeline.transformData(
          inputDF,
          settings.labelCol,
          settings.categoricalFeatures,
          settings.numericalFeatures
        )

        // Run the transformation pipeline on the dataset to prepare the data for model building
        val preparedDF: org.apache.spark.sql.DataFrame = transformPipeline.transform(inputDF);
        logger.info("Completed transforming data using the pipeline.")

        // split the prepared data into train-test datasets as: 90% and 10%
        val Array(trainingDF, testDF) = preparedDF.randomSplit(Array(0.9, 0.1))
        // At this point, these two datasets may be "cached" for improving Spark performance:
        trainingDF.cache()
        testDF.cache()

        // Step 4:
        // Fit multiple different models on this training data

        // Train a logistic regression model
        val lrModel = ModelPipeline.fitLRModel(trainingDF, "label")

        // Train a Gradient Boosted Decision Tree model
        val gbtModel = ModelPipeline.fitGBTRModel(trainingDF, "label")

        // Train a RandomForest Model
        val rfModel = ModelPipeline.fitRFModel(trainingDF, "label")

        // Step 5:
        // Evaluate each model's performance on test set
        // Make predictions on test data using the Transformer.transform() method.
        logger.info("Model performance evaluation for -> Logistic Regression:")
        // Note: model.transform will only use the 'features' column.
        val testResultLR = lrModel.transform(testDF);
        // custom processing to add class1 probability as a separate column for easier calculations
        val testResultLRWithProbsDF = ModelPipeline.addBinaryProbabilities(testResultLR)
        // evaluate model performance on test set
        ModelPipeline.evaluatePerformance(testResultLRWithProbsDF,
          labelColName = "label",
          class1ProbColName = "p1")

        logger.info("Model performance evaluation for -> Gradient Boosted Decision Trees Model:")
        val testResultGBDT = gbtModel.transform(testDF);
        val testResultGBDTWithProbsDF = ModelPipeline.addBinaryProbabilities(testResultGBDT)
        ModelPipeline.evaluatePerformance(testResultGBDTWithProbsDF,
          labelColName = "label",
          class1ProbColName = "p1")

        logger.info("Model performance evaluation for -> Random Forest Model:")
        val testResultRF = rfModel.transform(testDF);
        val testResultRFWithProbsDF = ModelPipeline.addBinaryProbabilities(testResultRF)
        ModelPipeline.evaluatePerformance(testResultRFWithProbsDF,
          labelColName = "label",
          class1ProbColName = "p1")

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
        val preparedDF: org.apache.spark.sql.DataFrame = transformPipeline.transform(inputDF);
        logger.info("Completed transforming data using the transformation pipeline.")

        // Alternatively, load an already fitted model from disk:
        val lrModel = org.apache.spark.ml.tuning.CrossValidatorModel.load(settings.lrModelSavePath)

        logger.info("Generating inferences for -> Logistic Regression:")
        val testResultLR = lrModel.transform(preparedDF);

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

}

