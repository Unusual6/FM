import backtrader as bt
import tushare as ts
import pandas as pd

# 1️⃣ 从 Tushare 获取历史数据
ts.set_token('你的token')
pro = ts.pro_api()
df = pro.daily(ts_code='600519.SH', start_date='20200101', end_date='20241231')

# 2️⃣ 整理成 Backtrader 能识别的格式（必须含 open, high, low, close, volume）
df.rename(columns={
    'trade_date': 'date',
    'vol': 'volume'
}, inplace=True)
df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
df.set_index('date', inplace=True)
df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
data_feed = bt.feeds.PandasData(dataname=df.sort_index())

# 3️⃣ 创建回测引擎，加入数据和策略
cerebro = bt.Cerebro()
cerebro.adddata(data_feed)
cerebro.addstrategy(MyStrategy)  # 你自定义的策略类

# 4️⃣ 运行回测
results = cerebro.run()
cerebro.plot()  # 画出收益曲线