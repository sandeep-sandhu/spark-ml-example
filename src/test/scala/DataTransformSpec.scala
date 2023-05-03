/**
  * DataTransformSpec:
  *
  *
  */
import ModelData.convertDFtoDS
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome}
import org.scalatest.funsuite.AnyFunSuite

// Enables setting up fixtures using before-all and after-all
class DataTransformSpec
    extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  self =>
  @transient var ss: SparkSession = null
  @transient var sc: SparkContext = null

  private object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.ss.sqlContext
  }

  override def beforeAll(): Unit = {
    val sparkConfig = new SparkConf().setAppName("DataTransform Test")
    sparkConfig.set("spark.broadcast.compress", "false")
    sparkConfig.set("spark.shuffle.compress", "false")
    sparkConfig.set("spark.sql.shuffle.partitions", "4")
    sparkConfig.set("spark.shuffle.spill.compress", "false")
    sparkConfig.set("spark.master", "local")

    ss = SparkSession.builder().config(sparkConfig).getOrCreate()

  }

  override def afterAll(): Unit =
    ss.stop()

  test("simple dataframe assert") {

    val df = ss
      .createDataFrame(
        Seq((1, "a string", "another string", 12344567L))
      )
      .toDF(
        "first val",
        "stringval",
        "stringval2",
        "longnum"
      )

    assert(df.count == 1)
  }

  test("Test the data transformation pipeline") {

    val inputDF = ss
      .createDataFrame(
        Seq(
          (
            25,
            "technician",
            "single",
            "university.degree",
            "no",
            "no",
            "yes",
            "telephone",
            "may",
            "mon",
            20,
            1,
            999,
            0,
            "nonexistent",
            1.1,
            93.994,
            -36.4,
            4.857,
            5191,
            "no"
          ),
          (
            50,
            "entrepreneur",
            "married",
            "university.degree",
            "unknown",
            "yes",
            "no",
            "telephone",
            "may",
            "mon",
            1042,
            1,
            999,
            0,
            "nonexistent",
            1.1,
            93.994,
            -36.4,
            4.857,
            5191,
            "yes"
          ),
          (
            10,
            "entrepreneur",
            "single",
            "university.degree",
            "unknown",
            "yes",
            "no",
            "telephone",
            "may",
            "mon",
            1042,
            1,
            999,
            0,
            "nonexistent",
            1.1,
            93.994,
            -36.4,
            4.857,
            5191,
            "no"
          )
        )
      )
      .toDF(
        "age",
        "job",
        "marital",
        "education",
        "defaulted",
        "housing",
        "loan",
        "contact_no",
        "month_name",
        "day_of_week",
        "duration",
        "campaign",
        "pdays",
        "previous",
        "poutcome",
        "emp_var_rate",
        "cons_price_idx",
        "cons_conf_idx",
        "euribor3m",
        "nr_employed",
        "y"
      )

    val inputDataset = convertDFtoDS(ss, inputDF);
    val num_feats = Array("age", "duration");
    val cat_feats = Array("job", "marital")

    val transformPipeline: PipelineModel = ModelPipeline.transformData(
      inputDataset,
      "y",
      cat_feats,
      num_feats
    );

    // Run the transformation pipeline on the dataset to prepare the data for model building
    val preparedDF: org.apache.spark.sql.DataFrame =
      transformPipeline.transform(inputDF)
    val collectedArray = preparedDF.collect();
    val colNames = preparedDF.columns;

    collectedArray.foreach { x =>
      //println(": " + x.toString);

      // encoding of target label "y"
      if (x.get(20).toString == "yes") {
        assert(x.get(21) == 1.0)
      } else {
        assert(x.get(21) == 0.0)
      }

      // encoding of job category:
      if (x.get(1).toString == "technician") {
        assert(x.get(22) == 1.0)
      } else {
        assert(x.get(22) == 0.0)
      }

      // encoding of marital status:
      if (x.get(2).toString == "married") {
        assert(x.get(23) == 1.0)
      } else {
        assert(x.get(23) == 0.0)
      }

    }

    // age scaling:

    // duration scaling:

  }
}

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session for scala tests")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

}
