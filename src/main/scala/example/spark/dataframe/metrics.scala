package example.spark.dataframe

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._
import org.apache.spark.util.SizeEstimator

case class DfConfig(var partitionMaxSizeMb: Int = 256,
                    var minNumOfPartitions: Int = 1,
                    var maxNumOfPartitions: Int = 200,
                    var compressionRatio: Double = 0.01)

case class DfMetrics(var recordCount: Long = 0,
                     var simpleColumnCount: Int = 0,
                     var complexColumnCount: Int = 0,
                     var rowSizeEstimate: Int = 0,
                     var memorySizeEstimateMb: Int = 0,
                     var diskSizeEstimateMb: Int = 0)

case class DfSuggestions(var suggestedNumPartitions: Int = 1)

class Metrics(df: DataFrame) {

  private val dfConfig = new DfConfig
  private val dfMetrics = new DfMetrics
  private val dfSuggestions = new DfSuggestions

  def partitionMaxSizeMb = { dfConfig.partitionMaxSizeMb }
  def minNumOfPartitions = { dfConfig.minNumOfPartitions }
  def maxNumOfPartitions = { dfConfig.maxNumOfPartitions }
  def compressionRatio = { dfConfig.compressionRatio }

  def recordCount = { dfMetrics.recordCount }
  def simpleColumnCount = { dfMetrics.simpleColumnCount }
  def complexColumnCount = { dfMetrics.complexColumnCount }
  def rowSizeEstimate = { dfMetrics.rowSizeEstimate }
  def memorySizeEstimateMb = { dfMetrics.memorySizeEstimateMb }
  def diskSizeEstimateMb = { dfMetrics.diskSizeEstimateMb }

  def suggestedNumPartitions = { dfSuggestions.suggestedNumPartitions }

  df.persist(StorageLevels.MEMORY_AND_DISK)

  dfMetrics.recordCount = df.count()
  dfMetrics.rowSizeEstimate = df.schema.fields.map{ c => getColumnWidth(df, c) }.sum

  dfMetrics.memorySizeEstimateMb = math.ceil(SizeEstimator.estimate(df) / 1024 / 1024).toInt
  dfMetrics.diskSizeEstimateMb = math.ceil(((dfMetrics.recordCount * dfMetrics.rowSizeEstimate) / 1024 / 1024) * dfConfig.compressionRatio).toInt

  dfSuggestions.suggestedNumPartitions = math.ceil(dfMetrics.diskSizeEstimateMb / dfConfig.partitionMaxSizeMb).toInt match {
    case c if c > dfConfig.maxNumOfPartitions => dfConfig.maxNumOfPartitions
    case c if c < dfConfig.minNumOfPartitions => dfConfig.minNumOfPartitions
    case c => c
  }


  /**
   * Calculated estimated column width
   * @param df: DataFrame to use for exploding ArrayType, MapType and StructType
   * @param sf: StructField
   * @return estimated width of StructField
   */
  private def getColumnWidth(df: DataFrame, sf: StructField): Int = {
    sf.dataType match {
      case ct@(_: ArrayType | _: MapType) =>
        dfMetrics.complexColumnCount = dfMetrics.complexColumnCount + 1
        df.select(explode(col(sf.name))).schema.map { c => getColumnWidth(df, c) }.sum
      case st: StructType =>
        dfMetrics.complexColumnCount = dfMetrics.complexColumnCount + 1
        df.select(col(sf.name + ".*")).schema.map { c => getColumnWidth(df, c) }.sum
      case dt: DataType =>
        dfMetrics.simpleColumnCount = dfMetrics.simpleColumnCount + 1
        dt.defaultSize
    }
  }

  override def toString: String = {
    implicit val formats = DefaultFormats
    s"""{ "DataFrameMetrics": { "config": ${write(dfConfig)}, "metrics": ${write(dfMetrics)}, "suggestions": ${write(dfSuggestions)} } }"""
  }
}