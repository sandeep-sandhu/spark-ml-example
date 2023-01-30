/**
 * File: AppSettings.scala
 * Purpose:
 *
 * */

import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.logging.log4j.scala.{Logger, Logging}
import org.apache.logging.log4j.Level

class AppSettings(config: Config) extends Serializable with org.apache.logging.log4j.scala.Logging {
  val inputFile: String = config.getString("file.input")
  val outputFile: String = config.getString("file.output")

  val jdbcDriver: String = config.getString("jdbc.driver")
  val jdbcUser: String = config.getString("jdbc.user")
  val jdbcPassword: String = config.getString("jdbc.password")
  val jdbcUrl: String = config.getString("jdbc.url")
  val jdbcTableName: String = config.getString("jdbc.tablename")

  val minPartitions: Int = config.getInt("minPartitions")
  val labelCol: String = config.getString("label.column")
  val numericalFeatures: Array[String] = config.getStringList("numerical.features").asScala.map( x => x).toArray
  val categoricalFeatures: Array[String] = config.getStringList("categorical.features").asScala.map(x => x).toArray
  val lrModelSavePath: String = config.getString("lr.model.save.path")
  val gbdtModelSavePath: String = config.getString("gbdt.model.save.path")
  val transformPipelineSavePath: String = config.getString("transform.pipeline.path")

}

