package p2p_data_analysis.spark.types

// Defines the schema for the input dataframe.
case class InfoType(
                     loan_amnt: Option[String],
                     term: Option[String],
                     int_rate: Option[String],
                     installment: Option[String],
                     home_ownership: Option[String],
                     annual_inc: Option[String],
                     DTI: Option[String],
                     addr_state: Option[String],
                     emp_length: Option[String],
                     title: Option[String],
                     has_collection: Option[Int]
                   )
