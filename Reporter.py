import datetime
import os


class Reporter(object):
    """
    The Reporter class has one main method: "report".
    We will use a separate Python script to call this Reporter Python class with desired arguments.
    It will inherit some object and take in a spark context object and configuration settings as input.

    We will build on the spark context object in order to read parquet file, select desired data, etc.
    """

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    # TODO: Change eod_dir to local directory.
    def report(self, spark, trade_date, eod_dir):
        """
        The report object will be used to report ___.

        :param spark: Spark session object.
        :param trade_date: Trade date of desired dataset.
        :param eod_dir: Directory where EOD Data lies.
        :return:
        """

        # Apply datetime conversion of trade_date column
        date = datetime.datetime.strptime(trade_date, "%Y-%m-%d")
        prev_date = date - datetime.strptime(days=1)
        prev_date_str = prev_date.strftime("%Y-%m-%d")

        # Trade
        df = spark.read.parquet(os.path.join(eod_dir, f"trade/trade_dt2={trade_date}"))\
            .selectExpr("trade_dt", "symbol", "exchange", "cast(event_tm as timestamp) as event_tm",
                        "event_seq_nb", "trade_pr")
        df.createOrReplaceTempView("tmp_trade_moving_avg")

        # Last Trade
        df = spark.read.parquet(os.path.join(eod_dir, f"trade/trade_dt2={prev_date_str}")) \
            .selectExpr("trade_dt", "symbol", "exchange", "cast(event_tm as timestamp) as event_tm",
                        "event_seq_nb", "trade_pr")
        df.createOrReplaceTempView("tmp_last_trade")

        # Quote
        df = spark.read.parquet(os.path.join(eod_dir, f"quote/trade_dt2={trade_date}")) \
            .selectExpr("trade_dt", "symbol", "exchange", "cast(event_tm as timestamp) as event_tm",
                        "event_seq_nb", "bid_pr", "bid_size", "ask_pr", "ask_size")
        df.createOrReplaceTempView("quotes")

        mov_avg_df = spark.sql("""
        select trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr,
        mean(trade_pr) OVER (PARTITION BY symbol, exchange ORDER BY event_tm 
        RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW
        from tmp_trade_moving_avg)
        """)

        mov_avg_df.createOrReplaceTempView("temp_trade_moving_avg")

        # Quote Union
        quote_union = spark.sql("""
        select trade_dt, 'Q' as rec_type, symbol, event_tm, event_seq_nb, exchange,
        bid_pr, bid_size, ask_pr, ask_size, null as trade_pr, null as mov_avg_pr
        from quotes
        union all
        select trade_dt, 'T' as rec_type, symbol, event_tm, event_seq_nb, exchange,
        null as bid_pr, null as bid_size, null as ask_pr, null as ask_size, trade_pr, mov_avg_pr
        from temp_trade_moving_avg
        """)

        quote_union_update = spark.sql("""
        select *,
        last_value(trade_pr, true) OVER (PARTITION BY symbol, exchange ORDER BT event_tm ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_trade_pr,
        last_value(mov_avg_pr, true) OVER (PARTITION BY symbol, exchange ORDER BT event_tm ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_mov_avg_pr
        from quote_union
        """)
        quote_union_update.createOrReplaceTempView("quote_union_update")

        # Quote Update
        quote_update = spark.sql("""
        select trade_dt, symbol, event_tm, event_seq_nb, exchange,
        bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr
        from quote_union_update
        where rec_type = 'Q'
        """)
        quote_update.createOrReplaceTempView("quote_union_update")
        # last_pr_df.filter("symbol = 'SYMA' and exchange = 'NYSE'")

        quote_final = spark.sql("""
        select trade_dt, symbol, event_tm, event_seq_nb, exchange, 
        bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr,
        bid_pr - close_pr as bid_pr_mv, ask_pr - close_pr as ask_pr_mv
        from (
        select /*+ BROADCAST(t) */ q.trade_dt, q.symbol, q.event_tm, q.event_seq_nb, q.exchange,
        q.bid_pr, q.bid_size, q.ask_pr, q.ask_size, q.last_trade_pr, q.last_mov_avg_pr,
        t.last_pr as close_pr
        from quote_update q
        left outer join temp_last_trade t on q.symbol = t.symbol and q.exchange = t.exchange
        ) a
        """)
        quote_final.write.mode("overwrite").parquet(os.path.join(eod_dir, f"quote-trade-analytical/trade_dt2={trade_date}"))
        return

    def save_as_csv(self, df, filename):
        df.write.mode("overwrite").csv(filename)
        return

