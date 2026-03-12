# FM

一个最小的量化选股 + 回测示例项目，选股使用 AkShare 免费数据源。

## 环境准备

建议 Python 3.10+。

安装依赖：

```bash
pip install -r requirements.txt
```

当前选股脚本 `filter.py` 使用 AkShare，无需 Tushare token。

## 量化选股

运行：

```bash
python filter.py
```

常用参数示例：

```bash
python filter.py \
  --lookback-days 120 \
  --min-listed-days 100 \
  --pe-max 30 \
  --momentum-min 0.10 \
  --vol-ratio-min 1.2 \
  --near-high-min 0.90 \
  --output quant_selected_stocks.csv \
  --log-level INFO
```

输出：

- 控制台会打印入选股票与跳过原因统计
- 结果保存为 CSV（默认 `quant_selected_stocks.csv`）

## 回测示例（Backtrader）

运行（默认对 `600519.SH` 做 SMA 金叉/死叉示例策略回测）：

```bash
python backtest.py
```

带参数与画图：

```bash
python backtest.py --ts-code 600519.SH --start-date 20200101 --end-date 20241231 --fast 10 --slow 30 --plot
```

## 注意事项

- Tushare 有限频/额度限制，选股脚本已做了最基础的 sleep 限频，但更推荐后续改成“按交易日批量拉全市场 + 本地缓存”的方式提速并降低限频风险。
