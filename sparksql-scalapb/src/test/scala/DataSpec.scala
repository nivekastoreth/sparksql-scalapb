
import com.example.protos.base.Base
import com.example.protos.demo._
import com.example.protos.oneof.{ABValue, AValue, OneOfTest}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, MustMatchers}

case class PersonLike(
  name: String, age: Int, addresses: Seq[Address], gender: Gender,
  tags: Seq[String] = Seq.empty, base: Option[Base] = None)

class DataSpec extends FlatSpec with MustMatchers with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder().appName("ScalaPB Demo").master("local[2]").getOrCreate()

  import spark.implicits._

  DemoProtoUdt.register()
  HandRolled.register()

  val GenderFromString: UserDefinedFunction = udf[Option[Gender], String](Gender.fromName)
  val GenderToString: UserDefinedFunction = udf[Option[String], Gender](g => Option(g).map(_.name))

  val TestPerson: Person = Person().update(
    _.name := "Owen M",
    _.age := 35,
    _.gender := Gender.MALE,
    _.addresses := Seq(
      Address().update(
        _.city := "San Francisco"
      )
    )
  )

  val TestOneOf: OneOfTest = OneOfTest().update(
    _.abValue := ABValue.defaultInstance.update(
      _.innerOf := ABValue.InnerOf.A(
        AValue.defaultInstance.update(
          _.strValue := "Test Value"
        )
      )
    )
  )

  override def afterAll(): Unit = {
    spark.stop()
  }

//  "Creating person dataset" should "work" in {
//    val s = Seq(Person().withName("Foo"), Person().withName("Bar"))
//
//    val ds = spark.sqlContext.createDataset(s)
//    ds.count() must be(2)
//  }
//
//  "Creating enum dataset" should "work" in {
//    val genders = Seq("MALE", "MALE", "FEMALE", null)
//    val gendersDS = spark.createDataset(genders).withColumnRenamed("value", "genderString")
//    val typedDF = gendersDS.withColumn("gender", GenderFromString(gendersDS("genderString")))
//    val localTypedDF: Array[Row] = typedDF.collect()
//    typedDF.filter(GenderToString($"gender") === "MALE").count() must be(2)
//    typedDF.filter(GenderToString($"gender") === "FEMALE").count() must be(1)
//    localTypedDF must be(
//      Array(
//        Row("MALE", Gender.MALE),
//        Row("MALE", Gender.MALE),
//        Row("FEMALE", Gender.FEMALE),
//        Row(null, null)))
//  }
//
//  "Dataset[Person]" should "work" in {
//    val ds: Dataset[Person] = spark.createDataset(Seq(TestPerson))
//    ds.where($"age" > 30).count() must be(1)
//    ds.where($"age" > 40).count() must be(0)
//    ds.where(GenderToString($"gender") === "MALE").count() must be(1)
//    ds.collect() must be(Array(TestPerson))
//    ds.toDF().as[Person].collect() must be (Array(TestPerson))
//  }
//
//  "as[Person]" should "work for manual building" in {
//    val pl = PersonLike(
//      name = "Owen M", age = 35, addresses=Seq.empty, gender = Gender.MALE)
//    val manualDF: DataFrame = spark.createDataFrame(Seq(pl))
//    manualDF.as[Person].collect()(0) must be(Person().update(
//      _.name := "Owen M",
//      _.age := 35,
//      _.gender := Gender.MALE
//    ))
//  }


  "Dataset[OneOfTest]" should "work" in {
    val ds: Dataset[OneOfTest] = spark.createDataset(Seq(TestOneOf))
    //    ds.where($"age" > 30).count() must be(1)
    //    ds.where($"age" > 40).count() must be(0)
    //    ds.where(GenderToString($"gender") === "MALE").count() must be(1)
    //    ds.collect() must be(Array(TestPerson))
    ds.toDF().as[OneOfTest].collect() must be (Array(TestOneOf))
  }
}
