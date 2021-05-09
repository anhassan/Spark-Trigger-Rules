package rule

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import schema.SourceTxn

object IncomingWireFollowedOutGoingInGivenDays {
  def transform(txn: Dataset[SourceTxn], limitAmount: Int, processingDate: String) = {
    import txn.sparkSession.implicits._
    val window = Window.partitionBy('aml_party_id)
    txn.where('txn_cat_code.isin("WDI", "WCI", "WDO", "WCO"))
      .withColumn("rule_processing_date", to_date(lit(processingDate)))
      .withColumn("rule_txn_date", to_date('txn_datetime))
      .withColumn("time_elapsed", abs(datediff('rule_processing_date, 'rule_txn_date)))
      .where('time_elapsed > 0)
      .withColumn("outgoing_wire_transfer_count",
        count(when('txn_cat_code.isin("WDO", "WCO"), 1).over(window)))
      .withColumn("total_incoming_wire_transfer_amt",
        sum(when('txn_cat_code.isin("WDI", "WCI"), 'txn_total_amt)).over(window))
      .where('total_incoming_wire_transfer_amt >= limitAmount and
        'outgoing_wire_transfer_count > 0)
  }

}
