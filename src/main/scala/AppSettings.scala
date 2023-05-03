/**
  * File: AppSettings.scala
  * Purpose: Reads the application configuration from the conf file.
  *
  * */
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import org.apache.log4j.{Level, Logger}

class AppSettings(config: Config) extends Serializable {

  val logger: Logger = Logger.getLogger(sparkApp.appName)
  logger.setLevel(Level.INFO)

  logger.info("Started loading configuration from the config file.")

  val inputFile: String = config.getString("file.input")
  val outputFile: String = config.getString("file.output")

  val jdbcDriver: String = config.getString("jdbc.driver")
  val jdbcUser: String = config.getString("jdbc.user")
  val jdbcPassword: String = config.getString("jdbc.password")
  val jdbcUrl: String = config.getString("jdbc.url")
  val jdbcTableName: String = config.getString("jdbc.tablename")

  val minPartitions: Int = config.getInt("minPartitions")
  val labelCol: String = config.getString("label.column")

  val numericalFeatures: Array[String] =
    config.getStringList("numerical.features").asScala.map(x => x).toArray

  val categoricalFeatures: Array[String] =
    config.getStringList("categorical.features").asScala.map(x => x).toArray
  val lrModelSavePath: String = config.getString("lr.model.save.path")
  val gbdtModelSavePath: String = config.getString("gbdt.model.save.path")

  val transformPipelineSavePath: String =
    config.getString("transform.pipeline.path")

}
