package p2p_data_analysis.spark.aggregator

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import p2p_data_analysis.spark.types.InfoType

/** Analyzes lending habits of different people groups by aggregating the transformed datasets.
 *  This program will be executed after LoanDataReader and RejDataReader under io folder in the
 *  driver program, meaning the input datasets already have the same schema.
 *
 * @return a dataframe with each row indicating the lending habit of a particular people group.
 */
trait DataAggregator extends Logging{
  def loanInfoAggregator(rejDs: Dataset[InfoType], loanDs: Dataset[InfoType], spark: SparkSession): DataFrame = {
    import spark.implicits._

    val unionedDs = rejDs.unionByName(loanDs)

    // Analyzes the lending habits of different people groups and outputs the result dataframe
    // groupBy: groups people by a certain set of traits
    val aggregatedDf = unionedDs.groupBy("term", "home_ownership", "addr_state", "title", "emp_length")
                                .agg(avg($"loan_amnt").as("avg_loan_amnt"),
                                     avg($"int_rate").as("avg_int_rate"),
                                     avg($"annual_inc").as("avg_annual_inc"),
                                     avg($"DTI").as("avg_DTI"),
                                     sum($"has_collection").as("sum_collection"),
                                     avg($"installment").as("avg_installment")
                                    )

    aggregatedDf
  }
}
