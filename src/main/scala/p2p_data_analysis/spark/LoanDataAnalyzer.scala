package p2p_data_analysis.spark

import p2p_data_analysis.spark.aggregator.DataAggregator
import p2p_data_analysis.spark.io.{AggDataWriter, LoanDataReader, RejDataReader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession


/**
 * Driver class of the project.
 */
object LoanDataAnalyzer extends Logging with DataAggregator with AggDataWriter with LoanDataReader with RejDataReader {
  /**
   *
   * @param args args(0): path where loan data is stored
   *             args(1): path where rejection data is stored
   *             args(2): path where output file will be written
   */
  def main (args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Invalid parameters")
    }

    //Creates the Spark session used by the whole project
    val spark = SparkSession.builder().appName("loan-data-analyzer").getOrCreate()

    //Saves input and output paths
    val loanDataPath = args(0)
    val rejDataPath = args(1)
    val resDataPath = args(2)

    val loanDs = readData(loanDataPath, spark)
    val rejDs = readRejectionData(rejDataPath, spark)
    val aggregatedDf = loanInfoAggregator(rejDs, loanDs, spark)

    writeLoanAggregatedData(aggregatedDf, resDataPath)
  }
}
