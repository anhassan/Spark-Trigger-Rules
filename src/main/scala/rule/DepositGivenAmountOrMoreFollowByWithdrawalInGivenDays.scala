package rule

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import schema.SourceTxn

object DepositGivenAmountOrMoreFollowByWithdrawalInGivenDays {
  def transform(txn: Dataset[SourceTxn], processingDate: String,
                limitDepositAmount: Int, totalDepositDays: Int) = {
    import txn.sparkSession.implicits._
    val window = Window.partitionBy('aml_party_id)
    txn.where('txn_cat_code.isin("DCR", "DWT", "DDB", "WAM", "WLT"))
      .withColumn("rule_processing_date", to_date(lit(processingDate)))
      .withColumn("rule_txn_date", to_date('txn_datetime))
      .withColumn("time_elapsed", datediff('rule_processing_date, 'rule_txn_date))
      .withColumn("is_valid_deposit", 'txn_cat_code.isin("DCR", "DWT", "DDB") and
        'time_elapsed < totalDepositDays)
      .withColumn("is_valid_withdraw", 'txn_cat_code.isin("WAM", "WLT") and
        'rule_txn_date === 'rule_processing_date)
      .where('is_valid_deposit or 'is_valid_withdraw)
      .withColumn("deposit_amount", sum(when('is_valid_deposit
        , 'txn_toal_amt)).over(window))
      .withColumn("withdraw_today", count(when('is_valid_withdraw, 1)).over(window))
      .where('withdraw_today > 0 and 'deposit_amount >= limitDepositAmount)
  }

}
