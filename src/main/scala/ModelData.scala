
import java.util.Properties

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.ml.linalg.VectorUDT
import com.esotericsoftware.kryo.Kryo
import org.apache.log4j.{Level, Logger}

case class ModelDataRecord(
                            age: Double,
                            job: String,
                            marital: String,
                            education: String,
                            defaulted: String,
                            housing: String,
                            loan: String,
                            contact_no: String,
                            month_name: String,
                            day_of_week: String,
                            duration: Double,
                            campaign: Double,
                            pdays: Double,
                            previous: Double,
                            poutcome: String,
                            emp_var_rate: Double,
                            cons_price_idx: Double,
                            cons_conf_idx: Double,
                            euribor3m: Double,
                            nr_employed: Double,
                            y: String
                          )

object ModelData extends {

  val appName: String = sparkApp.appName

  val logger: Logger = Logger.getLogger(sparkApp.appName)
  logger.setLevel(Level.INFO)

  private var dbConnProp = new Properties()

  /**
   * Prepares list of classes to be used for registering with Kryo serliazer
   * @return Array of classes
   */
  def prepareListOfKryoClasses(): Array[Class[_]] = {
    Array(
      classOf[ModelDataRecord],
      Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
      Class.forName("org.apache.spark.sql.types.StringType$"),
      Class.forName("org.apache.spark.sql.types.IntegerType$"),
      Class.forName("org.apache.spark.sql.types.ByteType$"),
      Class.forName("org.apache.spark.sql.types.DoubleType$"),
      Class.forName("org.apache.spark.sql.types.BooleanType$"),
      Class.forName("org.apache.spark.ml.linalg.VectorUDT"),
      Class.forName("org.apache.spark.ml.linalg.MatrixUDT"),
      //Class.forName("org.apache.spark.util.collection.OpenHashMap[]"),// <- cannot register this class, its protected
      classOf[org.apache.spark.sql.execution.datasources.WriteTaskResult],
      classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary],
      classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats],
      classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
      classOf[org.apache.spark.sql.types.Metadata],
      classOf[Array[org.apache.spark.sql.types.StructType]],
      classOf[org.apache.spark.sql.types.StructType],
      classOf[Array[org.apache.spark.sql.types.StructField]],
      classOf[org.apache.spark.sql.types.StructField],
      classOf[org.apache.spark.sql.types.StringType],
      classOf[org.apache.spark.sql.types.ArrayType]
    )
  }

  /**
   * Write dataframe to csv file, remove vector columns before writing as csv format cannot write vector columns
   *
   * @param testResultDF   Data to write
   * @param outputFileName Filename of output CSV file
   */
  def saveDataFrameToCSV(testResultDF: org.apache.spark.sql.DataFrame,
                         outputFileName: String): Unit = {

    // first, remove any vector columns since they cannot be written to a csv file:
    var droppedTestDF = testResultDF.drop("features", "numericalfeatures", "scaledfeatures",
      "rawPrediction", "probability", "idx_job", "idx_marital", "idx_education", "idx_defaulted",
      "idx_housing", "idx_loan", "idx_day_of_week", "idx_poutcome", "vec_idx_job", "vec_idx_marital",
      "vec_idx_education", "vec_idx_defaulted", "vec_idx_housing", "vec_idx_loan", "vec_idx_day_of_week",
      "vec_idx_poutcome")

    //write dataframe to output file:
    logger.info("Saving dataframe to file: " + outputFileName)

    droppedTestDF.coalesce(numPartitions = 1)
      .write.option("header", value = true)
      .mode(saveMode = "overwrite")
      .csv(outputFileName);

  }

  val bank_telemkt_schema: StructType = new StructType()
    .add("age", DoubleType, nullable = true)
    .add("job", StringType, nullable = true)
    .add("marital", StringType, nullable = true)
    .add("education", StringType, nullable = true)
    .add("defaulted", StringType, nullable = true)
    .add("housing", StringType, nullable = true)
    .add("loan", StringType, nullable = true)
    .add("contact_no", StringType, nullable = true)
    .add("month_name", StringType, nullable = true)
    .add("day_of_week", StringType, nullable = true)
    .add("duration", DoubleType, nullable = true)
    .add("campaign", DoubleType, nullable = true)
    .add("pdays", DoubleType, nullable = true)
    .add("previous", DoubleType, nullable = true)
    .add("poutcome", StringType, nullable = true)
    .add("emp_var_rate", DoubleType, nullable = true)
    .add("cons_price_idx", DoubleType, nullable = true)
    .add("cons_conf_idx", DoubleType, nullable = true)
    .add("euribor3m", DoubleType, nullable = true)
    .add("nr_employed", DoubleType, nullable = true)
    .add("y", StringType, nullable = true)

  /**
   * Read data from CSV file, apply the given schema.
   * @param spark The spark session
   * @param fileName The csv file to read from
   * @return Dataset with rows of class ModelDataRecord
   */
  def readDataFile(spark: SparkSession,
                   fileName: String,
                   numPartitions:Int = 6): Dataset[ModelDataRecord] = {
    import spark.implicits._

    val inputDF = spark.read
      .option("header", "true")
      .option("numPartitions", numPartitions)
      .schema(bank_telemkt_schema)
      .csv(fileName)
      .as[ModelDataRecord];

    inputDF
  }

  /**
   * Sets the JDBC parameters that are used by the database connection.
   * @param driver JDBC driver name
   * @param url JDBC URL to connect to the database
   * @param user Username for the database connection authentication
   * @param password Password for the database connection authentication
   */
  def setDBConnProperties(driver:String,
                          url:String,
                          user:String,
                          password:String):Unit = {
    dbConnProp.setProperty("driver", driver);
    dbConnProp.put("url", url)
    dbConnProp.put("user", user)
    dbConnProp.put("password", password)
  }

  /**
   * Opens a database connection and loads the data into a dataframe.
   * @param spark The spark session
   */
  def readFromJdbcConn(spark: SparkSession,
                       tableName: String): Dataset[ModelDataRecord] = {

    import spark.implicits._

    // Read from JDBC database connection
    logger.info("Reading from a database table via a JDBC connection.")

    val dbdata_df = spark.read
      .format("jdbc")
      //.schema(bank_telemkt_schema) // <-- disabling since schema not supported for DB2 tables
      .option("driver", dbConnProp.getProperty("driver"))
      .option("url", dbConnProp.getProperty("url"))
      .option("dbtable", tableName)
      .option("user", dbConnProp.getProperty("user"))
      .option("password", dbConnProp.getProperty("password"))
      .option("fetchsize", 1000)
      // the following options for partitioning must be either given together or omitted completely:
      .option("numPartitions", 6)
      .option("partitionColumn", "duration")
      .option("lowerBound", 0)
      .option("upperBound", 5000)
      // must invoke load to connect and load the data
      .load()

    logger.info(
      "Loaded data from database table \'%s\' into a dataframe with %d partitions".format(
        tableName,
        dbdata_df.rdd.getNumPartitions)
    );

    // convert all column names to lowercase for consistency:
    val newCols = dbdata_df.columns.map(x => x.toLowerCase() )
    val dfRenamed = dbdata_df.toDF(newCols: _*)

    dfRenamed.as[ModelDataRecord]
  }

}
