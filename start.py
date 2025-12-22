import yaml

with open("/dbfs/FileStore/Sathish/Inputs/alert_config.yaml", "r") as f:
    config = yaml.safe_load(f)

thresholds = config["thresholds"]
weights = config["risk_weights"]
paths = config["paths"]
write_cfg = config["write"]
rfq_fx = spark.read.csv(paths['rfq_input'], header=True, inferSchema=True)
rfq_fx.display()
dealer_trades = spark.read.csv(paths['trades_input'], header=True, inferSchema=True)
dealer_trades.display()
market_benchmark = spark.read.csv(paths['benchmark_input'], header=True, inferSchema=True)
market_benchmark.display()
from pyspark.sql.functions import *
lookback = thresholds["pre_trade_lookback_seconds"]

joined_df = rfq_fx.join(dealer_trades, (rfq_fx.dealer == dealer_trades.dealer) & (rfq_fx.pair == dealer_trades.pair), how = "left")
joined_df.display()

pre_trade_df = joined_df.withColumn("time_diff_seconds",unix_timestamp(col("rfq_time")) - unix_timestamp(col("trade_time")))
pre_trade_df.display()

pre_trade_alert_df = pre_trade_df.withColumn("pre_trade_alert",
                                             when(
                                                (col("time_diff_seconds") > 0) &
                                                (col("time_diff_seconds") <= lookback) &
                                                (col("source") == "DealerOwn"),1
                                                ).otherwise(0)
                                             )
pre_trade_alert_df.display()
benchmark_alert_df = pre_trade_alert_df.withColumn(
    "benchmark_deviation_pips", abs(col("dealer_quote") - col("market_mid")) * 10000)
benchmark_alert_df.display()

benchmark_alert_df = benchmark_alert_df.withColumn(
    "benchmark_alert", when(
        col("benchmark_deviation_pips") > thresholds["pip_deviation_threshold"], 1).otherwise(0))
benchmark_alert_df.display()
latency_alert_df = benchmark_alert_df.withColumn(
    "latency_alert",
    when(col("latency_ms") > thresholds["latency_threshold_ms"], 1).otherwise(0))
latency_alert_df.display()
execution_alert_df = latency_alert_df.withColumn(
    "execution_slippage_pips", abs(col("client_exec_price") - col("market_mid")) * 10000)
execution_alert_df.display()

execution_alert_df = execution_alert_df.withColumn(
    "execution_quality_alert",
    when(col("execution_slippage_pips") > thresholds["slippage_threshold_pips"],1).otherwise(0))
execution_alert_df.display()
risk_df = execution_alert_df.withColumn(
    "risk_score",
    col("pre_trade_alert") * lit(weights["pre_trade"]) +
    col("benchmark_alert") * lit(weights["benchmark"]) +
    col("latency_alert") * lit(weights["latency"]) +
    col("execution_quality_alert") * lit(weights["execution_quality"])
)
risk_df.display()

risk_df = risk_df.withColumn(
    "risk_reasons",
    array(
        when(col("pre_trade_alert") == 1, lit("PRE_TRADE")),
        when(col("benchmark_alert") == 1, lit("BENCHMARK_DEVIATION")),
        when(col("latency_alert") == 1, lit("LATENCY")),
        when(col("execution_quality_alert") == 1, lit("EXECUTION_QUALITY"))
    )
)
risk_df.display()

risk_df = risk_df.withColumn(
    "risk_reasons",
    expr("filter(risk_reasons, x -> x is not null)")
)
risk_df.display()
df_to_write = risk_df.withColumn(
    "risk_reasons",
    concat_ws(";", col("risk_reasons"))
)

if write_cfg["csv"]:
    df_to_write.write.options(header=True).mode("overwrite").csv(paths["rfq_output"])

if write_cfg["parquet"]:
    risk_df.write.mode("overwrite").parquet(paths["rfq_output"])
summary_df = df_to_write.agg(
    expr("count(*) as total_rfqs"),
    expr("sum(pre_trade_alert) as pre_trade_alerts"),
    expr("sum(benchmark_alert) as benchmark_alerts"),
    expr("sum(latency_alert) as latency_alerts"),
    expr("sum(execution_quality_alert) as execution_quality_alerts"),
    expr("sum(case when risk_score >= 5 then 1 else 0 end) as high_risk_rfqs")
)
summary_df.write.mode("overwrite").json("dbfs:/FileStore/Sathish/Outputs/summary_alerts_json")
summary_df.display()
