package example.spark.partition_utils

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._

object PartitionUtils {

  /**
   * Calculated estimated column width
   * @param df: DataFrame to use for exploding ArrayType, and MapType columns
   * @param sf: StructField
   * @return estimated width of StructField
   */
  def getColumnWidth(df: DataFrame, sf: StructField): Int = {
    sf.dataType match {
      case at: ArrayType => df.select(explode(col(sf.name))).schema.map{ c => getColumnWidth(df, c) }.sum
      case mt: MapType => df.select(explode(col(sf.name))).schema.map { c => getColumnWidth(df, c) }.sum
      case dt: DataType => dt.defaultSize
      case st: StructType => df.select(col(sf.name)).schema.map{c => getColumnWidth(df, c) }.sum
    }
  }

  /**
   * Calculate estimated row width based on schema and it's data types.
   * @param df DataFrame
   * @return
   */
    def getEstimatedRowWidth(df: DataFrame) : Int = {
      df.schema.fields.map{c => getColumnWidth(df, c) }.sum
    }

    /**
     *
     * @param dfRecordCount: Count of records in Dataframe
     * @param dfRecordWidth: estimated Record Width
     * @param minNumOfPartitions: Default of 1
     * @param maxNumOfPartitions: Default of 200
     * @param compressionRatio: Default of 1%
     * @return Suggested Number of Partitions to use
     */
    def calculateNumPartitionsToUse(dfRecordCount: BigInt,
                                    dfRecordWidth: Int,
                                    partitionMaxSizeMB: Int = 256,
                                    minNumOfPartitions: Int = 1,
                                    maxNumOfPartitions: Int = 200,
                                    compressionRatio: Double = .01): Int = {

      val estimatedTotalSizeOnDiskMB = ((dfRecordCount * dfRecordWidth) / 1024 / 1024).toLong
      val estimatedTotalSizeOnDiskMBCompressed = (estimatedTotalSizeOnDiskMB * compressionRatio).toLong
      val suggestedNumOfPartitions = math.ceil(estimatedTotalSizeOnDiskMBCompressed / partitionMaxSizeMB).toInt

      suggestedNumOfPartitions match {
        case _ if suggestedNumOfPartitions > maxNumOfPartitions => maxNumOfPartitions
        case _ if suggestedNumOfPartitions < minNumOfPartitions => minNumOfPartitions
        case _ => suggestedNumOfPartitions
      }
    }

  /**
   * Provided a DataFrame, we will calculate the the record count and the suggested number of partitions to use for writing back to disk.
   * @param df: DataFrame to use
   * @return a Tuple with (Record Count, Estimated Row Width, Suggested Number Of Partitions)
   */
  def calculateNumPartitionsToUse(df: DataFrame): (Long, Int, Int) = {
    df.persist(StorageLevels.MEMORY_AND_DISK)
    val dfCount = df.count()
    val dfEstimatedRowWidth = getEstimatedRowWidth(df)
    val suggestedNumPartitions = calculateNumPartitionsToUse(dfCount, dfEstimatedRowWidth)
    (dfCount, dfEstimatedRowWidth, suggestedNumPartitions )
  }

}
