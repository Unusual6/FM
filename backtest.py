import argparse
import os
from datetime import datetime

import backtrader as bt
import pandas as pd
import tushare as ts


def get_pro() -> "ts.pro_api":
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise SystemExit("Missing TUSHARE_TOKEN. Set env var before running.")
    ts.set_token(token)
    return ts.pro_api()


def fetch_daily_df(pro, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        raise ValueError(f"No daily data for {ts_code} [{start_date}, {end_date}]")

    df = df.copy()
    df.rename(columns={"trade_date": "date", "vol": "volume"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    df.set_index("date", inplace=True)
    df = df[["open", "high", "low", "close", "volume"]].astype(float).sort_index()
    return df


class SmaCrossStrategy(bt.Strategy):
    params = dict(fast=10, slow=30)

    def __init__(self):
        sma_fast = bt.ind.SMA(self.data.close, period=self.p.fast)
        sma_slow = bt.ind.SMA(self.data.close, period=self.p.slow)
        self.cross = bt.ind.CrossOver(sma_fast, sma_slow)

    def next(self):
        if not self.position and self.cross > 0:
            self.buy()
        elif self.position and self.cross < 0:
            self.sell()


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Backtrader backtest with Tushare daily data")
    p.add_argument("--ts-code", type=str, default="600519.SH")
    p.add_argument("--start-date", type=str, default="20200101")
    p.add_argument("--end-date", type=str, default=datetime.today().strftime("%Y%m%d"))
    p.add_argument("--cash", type=float, default=100000.0)
    p.add_argument("--commission", type=float, default=0.001)
    p.add_argument("--fast", type=int, default=10)
    p.add_argument("--slow", type=int, default=30)
    p.add_argument("--plot", action="store_true")
    return p


def main(argv=None) -> None:
    args = build_arg_parser().parse_args(argv)
    pro = get_pro()
    df = fetch_daily_df(pro, ts_code=args.ts_code, start_date=args.start_date, end_date=args.end_date)

    data_feed = bt.feeds.PandasData(dataname=df)

    cerebro = bt.Cerebro()
    cerebro.adddata(data_feed)
    cerebro.broker.setcash(args.cash)
    cerebro.broker.setcommission(commission=args.commission)
    cerebro.addstrategy(SmaCrossStrategy, fast=args.fast, slow=args.slow)

    print(f"Start Portfolio Value: {cerebro.broker.getvalue():.2f}")
    cerebro.run()
    print(f"Final Portfolio Value: {cerebro.broker.getvalue():.2f}")

    if args.plot:
        cerebro.plot()


if __name__ == "__main__":
    main()