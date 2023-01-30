/**
 * File: ExploratoryAnalysis.scala
 * Purpose: Exploratory Data Analysis
 *
 */

import org.apache.logging.log4j.scala.{Logger, Logging}
import org.apache.logging.log4j.Level

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, expr}

import sparkApp.appName

object ExploratoryAnalysis extends org.apache.logging.log4j.scala.Logging{

  val spark: SparkSession = SparkSession.active
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  def exploreDF(inputDF: Dataset[ModelDataRecord],
                labelColName:String,
                categoricalFeatures: Array[String],
                numericalFeatures: Array[String]): Unit = {

    logger.info("Starting data exploration")

    // prints the schema of the dataframe:
    inputDF.printSchema()

    // Note: this triggers a calculation on the dataframe
    val totalRowCount = inputDF.count()
    logger.info("Data loaded with %d rows.".format(totalRowCount))

    // run any type of SQL queries on the dataframe:
    inputDF.createOrReplaceTempView("input")
    spark.sql("SELECT * from input").show(5);

    println("Showing the target distribution for the categorical variables:")
    categoricalFeatures.foreach( x => {
      println("Target variable cross-tab for categorical variable: " + x);
      val contTabDF = inputDF.stat.crosstab(labelColName, x);
      contTabDF.show();
    })

    println("Showing the numerical variables distribution for each value of the target:")
    numericalFeatures.foreach(x => {
      println("When for target = no, numerical variable %s distribution: ".format(x));
      inputDF.where(inputDF(labelColName) === "no").describe(labelColName, x).show()

      println("When for target = yes, numerical variable %s distribution: ".format(x));
      inputDF.where(inputDF(labelColName) === "yes").describe(labelColName, x).show()
    })

    this.plotCharts(inputDF)
  }

  def plotCharts(inputDF: Dataset[ModelDataRecord]): Unit={
    // TODO: save interesting .svg plots
  }

}
