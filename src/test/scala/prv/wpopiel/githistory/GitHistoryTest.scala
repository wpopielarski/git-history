package prv.wpopiel.githistory

import org.apache.spark.sql.types._
import scala.collection.JavaConverters._
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

class GitHistoryTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  var ss: SparkSession = _

  override def beforeAll(): Unit = {
    ss = SparkSession
         .builder
         .config("spark.ui.enabled", "false")
         .appName("testApp")
         .master("local[*]")
         .getOrCreate()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    ss.stop()
    super.afterAll()
  }

  "pathFinder" should "find paths" in {
    val tested = StructType(List(
      StructField("not this", IntegerType),
      StructField("inside", StructType(Seq(
        StructField("not this", IntegerType),
        StructField("this one", StructType(Seq.empty[StructField]))
      ))),
      StructField("this one", StructType(Seq.empty[StructField]))
    ))

    val result = GitHistory.pathFinder(tested, "this one", "root")

    result should contain allOf ("root.inside.this one", "root.this one")
  }

  it should "find paths without root" in {
    val tested = StructType(List(
      StructField("not this", IntegerType),
      StructField("inside", StructType(Seq(
        StructField("not this", IntegerType),
        StructField("this one", StructType(Seq.empty[StructField]))
      ))),
      StructField("this one", StructType(Seq.empty[StructField]))
    ))

    val result = GitHistory.pathFinder(tested, "this one", "")

    result should contain allOf ("inside.this one", "this one")
  }

  it should "not find any path" in {
    val tested = StructType(List(
      StructField("not this", IntegerType),
      StructField("inside", StructType(Seq(
        StructField("not this", IntegerType),
        StructField("not even this", StructType(Seq.empty[StructField]))
      ))),
      StructField("not even this", StructType(Seq.empty[StructField]))
    ))

    val result = GitHistory.pathFinder(tested, "this one", "root")

    result shouldBe empty
  }

  "allDataframes" should "collect all objects" in {
    val json = Seq(
    """{"a": 1, "b": {"pickMe": {"c": "text1"}}, "created_at":0}""",
    """{"a": 2, "pickMe": {"c": "text2"}, "created_at":0}"""
    )
    val spark = ss
    import spark.implicits._
    val input = ss.createDataset(json)
    val tested = spark.read.json(input)
    tested.printSchema()

    val actual = GitHistory.allDataframes(tested, Seq("b.pickMe", "pickMe"))
    actual.where($"pickMe".isNotNull).toJSON.collect().foreach(r => println(r.mkString))

    actual.toJSON.collect().toSet should contain allOf ("""{"pickMe":{"c":"text1"},"created_at":0}""",
                                                        """{created_at":0}""",
                                                        """{"pickMe":{"c":"text2"},"created_at":0}""")
  }

  "makeName" should "not add prefix" in {
    val emptyPath = ""

    GitHistory.makeName(emptyPath, "suffix") shouldBe "suffix"
  }

  it should "add path" in {
    val path = "path"

    GitHistory.makeName(path, "suffix") shouldBe "path.suffix"
  }
}
