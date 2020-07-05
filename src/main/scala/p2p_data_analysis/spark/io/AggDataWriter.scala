package p2p_data_analysis.spark.io

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

/**
 * Write the aggregated dataframe to a json file to be saved in outputPath
 */
trait AggDataWriter {
  def writeLoanAggregatedData(outputDf: DataFrame, outputPath: String): Unit = {
    outputDf.repartition(1).write.json(outputPath) // Ease checking by writing to the same file.
  }
}
