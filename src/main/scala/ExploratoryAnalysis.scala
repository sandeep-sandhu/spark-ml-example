/**
  * File: ExploratoryAnalysis.scala
  * Purpose: Exploratory Data Analysis
  *
  */
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.ml.stat.KolmogorovSmirnovTest
import org.apache.log4j.{Level, Logger}
import breeze.linalg._
import breeze.plot._
import org.apache.spark.sql.functions.{col, udf, _}

object ExploratoryAnalysis {

  val logger: Logger = Logger.getLogger(sparkApp.appName)
  logger.setLevel(Level.INFO)

  val spark: SparkSession = SparkSession.active
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /**
    * Calculate and print various univariate and bivariate statistics on the given dataset.
    * @param inputDF Dataset to be analysed.
    * @param labelColName Name of the target column
    * @param categoricalFeatures List of categorical variable names
    * @param numericalFeatures List of numerical variable names
    */
  def exploreDF(
    inputDF: Dataset[ModelDataRecord],
    labelColName: String,
    categoricalFeatures: Array[String],
    numericalFeatures: Array[String]
  ): Unit = {

    logger.info("Starting data exploration")

    // prints the schema of the dataframe:
    inputDF.printSchema()

    // Note: this triggers a calculation on the dataframe
    val totalRowCount = inputDF.count()
    logger.info(s"Data loaded with $totalRowCount rows.")

    // run any type of SQL queries on the dataframe:
    inputDF.createOrReplaceTempView("input")
    spark.sql("SELECT * from input").show(5)

    logger.info(
      "Showing the target distribution for all the categorical variables:"
    )
    categoricalFeatures.foreach { x =>
      logger.info(s"Target variable cross-tab for categorical variable: $x")
      val contTabDF = inputDF.stat.crosstab(labelColName, x)
      contTabDF.show()
    }

    logger.info(
      "Showing the numerical variables distribution for each value of the target:"
    )
    numericalFeatures.foreach { x =>
      logger.info("Numerical variable %s distribution: ".format(x))
      numericalBivariateTable(inputDF, labelColName, x, "no", "yes").show()
    }

    plotCharts(inputDF)
  }

  /**
    * Convenience function to compare statistics on numerical column vs. the target values.
    * It compares the two samples of a continuous variable - one sample each for the target variable's two classes.
    *
    * The two-sample Kolmogorov-Smirnov (K-S) test compares and gives the difference between the CDF (cumulative distribution functions) of two samples.
    * However, Spark MLlib does not have a two-sample K-S test function.
    * As a workaround, the one-sample K-S test is applied here to compare each sample one by one against the normal distribution CDF
    * with given mean and standard deviation.
    *
    * @param inputData Dataset to analyse numerical variables
    * @param labelColName Column name of the target attribute
    * @param colToAnalyse Numerical variable to be analysed
    * @param label1 Target variable value for Class-0
    * @param label2 Target variable value for Class-1
    * @return Dataframe with statistics on each population group - class 0 vs. class 1
    */
  private def numericalBivariateTable(
    inputData: Dataset[_],
    labelColName: String,
    colToAnalyse: String,
    label1: String,
    label2: String
  ): DataFrame = {

    val statsDF1 = inputData
      .where(inputData(labelColName) === label1)
      .describe(colToAnalyse)
      .withColumnRenamed("summary", "statistics_1")
      .withColumnRenamed(
        colToAnalyse,
        colToAnalyse + "_" + labelColName + "_" + label1
      )

    val statsDF2 = inputData
      .where(inputData(labelColName) === label2)
      .describe(colToAnalyse)
      .withColumnRenamed("summary", "statistics_2")
      .withColumnRenamed(
        colToAnalyse,
        colToAnalyse + "_" + labelColName + "_" + label2
      )

    val joinedDF = statsDF1
      .join(
        statsDF2,
        statsDF1("statistics_1") === statsDF2("statistics_2"),
        "inner"
      )
      .drop("statistics_2")
      .withColumnRenamed("statistics_1", "Statistic")

    val tmpArray = joinedDF.collect()
    val meanVal = tmpArray(1)(1).toString.toDouble
    val sdVal = tmpArray(2)(1).toString.toDouble

    val ksTestResult1 = KolmogorovSmirnovTest
      .test(
        inputData.where(inputData(labelColName) === label1),
        colToAnalyse,
        "norm",
        meanVal,
        sdVal
      )
      .take(1)(0)
    //val ksPvalue1 = ksTestResult1.getDouble(0)
    val ksStatistic1 = ksTestResult1.getDouble(1)

    val ksTestResult2 = KolmogorovSmirnovTest
      .test(
        inputData.where(inputData(labelColName) === label2),
        colToAnalyse,
        "norm",
        meanVal,
        sdVal
      )
      .take(1)(0)
    // ksPvalue2 = ksTestResult2.getDouble(0)
    val ksStatistic2 = ksTestResult2.getDouble(1)

    joinedDF.union(Seq(("K-S Statistic", ksStatistic1, ksStatistic2)).toDF)
  }

  def plotCharts(inputDF: Dataset[ModelDataRecord]): Unit = {
    // TODO: create and save interesting plots:

    val f = Figure()
    f.visible = false
    val p = f.subplot(0)
    val x = linspace(0.0, 1.0)
    p += plot(x, x ^:^ 2.0)
    p += plot(x, x ^:^ 3.0, '.')
    p.title = "Some nice chart"
    p.xlabel = "x axis"
    p.ylabel = "y axis"
    f.saveas("lines.png")

  }

}
