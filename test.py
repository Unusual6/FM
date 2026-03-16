from tickflow import TickFlow

tf = TickFlow.free()  # 免费服务

# 获取日K线
df = tf.klines.get("600000.SH", period="1d", count=100, as_dataframe=True)
print(df.tail())

# 获取标的信息
instruments = tf.instruments.batch(symbols=["600000.SH", "000001.SZ"])
print(instruments)

# pip install "tickflow[all]" --upgrade
