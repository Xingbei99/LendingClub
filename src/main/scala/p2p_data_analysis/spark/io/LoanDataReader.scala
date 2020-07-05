package p2p_data_analysis.spark.io

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import p2p_data_analysis.spark.types.InfoType

/**
 * Transforms lending data to a dataset of the specified schema.
 * @return: A dataset of the defined uniform schema.
 */
trait LoanDataReader extends Logging{
  def readData(inputPath: String, spark: SparkSession): Dataset[InfoType] = {
    import spark.implicits._

    // Reads raw data into a dataframe.
    val rawData = spark.read.option("header", "true").csv(inputPath)
    logInfo("Reading loan data from %s".format(inputPath))
    val filteredRawDf = rawData.filter($"loan_status" =!= "Fully Paid") // Filtered completed loans

    //Transforms the dataframe to the specified schema and output it as a dataset.
    val fields = List("loan_amnt", "term", "int_rate", "installment", "home_ownership",
                      "annual_inc", "emp_length", "title", "addr_state", "loan_status", "tot_coll_amt").map(col)
    filteredRawDf.select(fields: _*)
                 .withColumn("has_collection",
                             when($"tot_coll_amt" =!= "0", 1).otherwise(0))
                 .withColumn("DTI", $"installment" / ($"annual_inc" / 12))
                 .drop("loan_status")
                 .drop("tot_coll_amt")
                 .as[InfoType] // convert a dataframe to a dataset
  }
}
