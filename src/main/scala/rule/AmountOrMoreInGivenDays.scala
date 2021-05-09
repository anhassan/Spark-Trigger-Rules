package rule

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import schema.SourceTxn

object AmountOrMoreInGivenDays {
  def transform(txn: Dataset[SourceTxn], limitAmount: Int, txnCatCodes: Seq[String]
                , daysRange: Int, processingDate: String) = {
    import txn.sparkSession.implicits._
    val window = Window.partitionBy('aml_party_id)
    txn.withColumn("rule_processing_date", to_date(lit(processingDate)))
      .withColumn("rule_txn_date", to_date('txn_datetime))
      .withColumn("time_elapsed", abs(datediff('rule_processing_date, 'rule_txn_date)))
      .where('txn_cat_code.isin(txnCatCodes: _*) and 'time_elapsed < daysRange)
      .withColumn("total_deposits", sum('txn_total_amt).over(window))
      .withColumn("txn_today", count(when('rule_processing_date === 'rule_txn_date, 1)))
      .where('txn_today > 0 and 'total_deposits >= limitAmount)
  }

}
