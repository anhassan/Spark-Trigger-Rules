package rule

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import schema.SourceTxn

object GivenWiresTotallingOrMoreInCurrencies {
  def transform(txn: Dataset[SourceTxn], currencyList: List[String], limitAmount: Int,
                minTxnCount: Int, maxTxnCount: Int, processingDate: String) = {
    import txn.sparkSession.implicits._
    val window = Window.partitionBy('aml_party_id)
    txn.where('txn_cat_code.isin("WDI", "WCI", "WDO", "WCO"))
      .withColumn("rule_processing_date", to_date(lit(processingDate)))
      .withColumn("rule_txn_date", to_date('txn_datetime))
      .withColumn("time_elapsed", abs(datediff('rule_processing_date, 'rule_txn_date)))
      .withColumn("is_valid_txn", 'orig_currency_code.isin(currencyList: _*) and 'time_elapsed === 0)
      .withColumn("wire_transfer_count", count(when('is_valid_txn, 1)).over(window))
      .withColumn("total_wire_transfer_amt",
        sum(when('is_valid_txn, 'txn_total_amt)).over(window))
      .where('wire_transfer_count.between(minTxnCount, maxTxnCount) and
        'total_wire_transfer_amt >= limitAmount)
  }

}
