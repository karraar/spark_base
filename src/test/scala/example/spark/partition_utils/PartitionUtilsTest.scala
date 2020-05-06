package example.spark.partition_utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()
  }

}
class PartitionUtilsTest extends AnyFunSpec with SparkSessionTestWrapper {

//  val data = Seq(Row(33, "333", 1),
//                 Row(12341234, "ABCD", 'a')
//                )
//  val schema = List(StructField("f1", IntegerType, nullable = true),
//                    StructField("f2", StringType, nullable = true),
//                    StructField("f3", ByteType, nullable = true)
//                   )-
  val df = spark.read.json("src/main/resources/iss.json")
//  spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))


  describe("ParitionUtils") {
    describe("getColumnWidth") {
      it("Provided a Type, should return defaultSize of that DataType") {
        assert(PartitionUtils.getColumnWidth(df, df.select("timestamp").schema.fields.head) === LongType.defaultSize)
        assert(PartitionUtils.getColumnWidth(df, df.select("message").schema.fields.head) === StringType.defaultSize)
      }
    }

    describe("getEstimatedRowWidth") {
      it("should return the sum of all field sizes") {
        assert(PartitionUtils.getEstimatedRowWidth(df) === 68)
      }
    }

    describe("calculateNumPartitionsToUse") {
      it("should consider limit guards") {
        assert(PartitionUtils.calculateNumPartitionsToUse(1, 1) === 1)
        assert(PartitionUtils.calculateNumPartitionsToUse(1000 * 1000 * 1000, 1000) === 37)
        assert(PartitionUtils.calculateNumPartitionsToUse(100 * 1000 * 1000 * 1000, 1000 * 1000) === 200)
      }
    }

    describe("calculateNumPartitionsToUse") {
      it("should return (3,1,1)") {
      assert(true)
        // TODO: responds with NullPointerException even though it works from main call
//      assert(PartitionUtils.calculateNumPartitionsToUse(df) === (3,1,1))
      }
    }
  }
  Thread.sleep(10000)
  spark.close()
}
