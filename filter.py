import argparse
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict

import akshare as ak
import numpy as np
import pandas as pd


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


_indicator_cache: Dict[str, pd.DataFrame] = {}


def get_stock_pool(use_cache: bool, cache_path: str) -> pd.DataFrame:
    """
    获取当前所有正常交易的A股股票列表（AkShare 实时行情快照）

    使用 ak.stock_zh_a_spot_em()，返回列中包含：
    - 代码（如 600519）
    - 名称
    - 今开、最高、最低、最新价、成交量等
    """
    # 若启用缓存且缓存存在，优先尝试读取缓存
    if use_cache and os.path.exists(cache_path):
        try:
            cached = pd.read_csv(cache_path)
            logging.info("已从缓存加载股票池: %s (共 %d 只)", cache_path, len(cached))
            return cached
        except Exception:
            logging.warning("读取股票池缓存失败，将尝试从 AkShare 获取最新数据", exc_info=True)

    # 优先尝试 Eastmoney 源，失败则回退到另一数据源
    df = None
    last_err: Exception | None = None
    for fn_name in ("stock_zh_a_spot_em", "stock_zh_a_spot"):
        try:
            fn = getattr(ak, fn_name)
        except AttributeError:
            continue
        try:
            logging.info("尝试通过 AkShare.%s 获取股票池...", fn_name)
            df = fn()
            if df is not None and not df.empty:
                break
        except Exception as e:
            last_err = e
            logging.warning("通过 AkShare.%s 获取股票池失败: %s", fn_name, e)

    if df is None or df.empty:
        logging.error("从 AkShare 获取股票池失败: %s", last_err)
        if use_cache and os.path.exists(cache_path):
            try:
                cached = pd.read_csv(cache_path)
                logging.warning("使用缓存股票池继续运行: %s (共 %d 只)", cache_path, len(cached))
                return cached
            except Exception:
                logging.error("缓存股票池也无法读取，请检查网络或稍后重试", exc_info=True)
        raise SystemExit("无法从 AkShare 获取股票池，也没有可用缓存，请检查网络或稍后重试。")

    # 统一字段命名，便于后续处理
    df = df.rename(
        columns={
            "代码": "symbol",
            "名称": "name",
        }
    )

    if use_cache:
        try:
            df.to_csv(cache_path, index=False, encoding="utf_8_sig")
            logging.info("已将股票池缓存到: %s", cache_path)
        except Exception:
            logging.warning("写入股票池缓存失败（不影响本次运行）", exc_info=True)

    return df


def get_daily_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    使用 AkShare 获取某只股票最近一段时间的日线数据（前复权）

    AkShare 代码格式为 600519、000001 等，不带交易所后缀。
    """
    # AkShare stock_zh_a_hist 的 end_date 为 "YYYYMMDD" 或 "YYYY-MM-DD"，这里沿用 YYYYMMDD
    df = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust="qfq",
    )
    if df is None or df.empty:
        return df

    df = df.rename(
        columns={
            "日期": "trade_date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "vol",
            "成交额": "amount",
        }
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date").reset_index(drop=True)
    return df

def calculate_factors(df: pd.DataFrame):
    """计算量化因子"""
    if len(df) < 20:
        return None
    
    close = df['close'].astype(float)
    volume = df['vol'].astype(float)
    
    # 1. 动量因子：20日收益率
    momentum = (close.iloc[-1] / close.iloc[-20] - 1) if len(close) >= 20 else 0
    
    # 2. 成交量因子：近期成交量是否放大（5日均量 / 20日均量）
    vol_5 = volume.tail(5).mean()
    vol_20 = volume.tail(20).mean()
    vol_ratio = vol_5 / vol_20 if vol_20 > 0 else 0
    
    # 3. 价格处于近期高位（收盘价接近20日最高）
    high_20 = df['high'].tail(20).astype(float).max()
    price_near_high = (close.iloc[-1] / high_20) if high_20 > 0 else 0
    
    return {
        'momentum': momentum,
        'vol_ratio': vol_ratio,
        'price_near_high': price_near_high
    }

def _get_lg_indicator(symbol: str) -> pd.DataFrame | None:
    """获取乐咕数据的估值/股息指标，并做简单缓存。"""
    if symbol in _indicator_cache:
        return _indicator_cache[symbol]

    df = None
    try:
        # 不同版本 AkShare 函数名可能略有差异，两个都尝试
        if hasattr(ak, "stock_a_indicator_lg"):
            df = ak.stock_a_indicator_lg(symbol=symbol)
        else:
            df = ak.stock_a_lg_indicator(stock=symbol)
    except Exception:
        logging.debug("indicator fetch failed for %s", symbol, exc_info=True)
        df = None

    if df is None or df.empty:
        return None

    _indicator_cache[symbol] = df
    return df


def get_pe_ratio(symbol: str) -> float:
    """使用 AkShare 获取市盈率（TTM），取不到则返回 NaN。"""
    df = _get_lg_indicator(symbol)
    if df is None or df.empty:
        return np.nan

    df = df.sort_values("trade_date")
    pe = df.iloc[-1].get("pe_ttm")
    return float(pe) if pd.notna(pe) and float(pe) > 0 else np.nan


def get_dividend_yield(symbol: str) -> float:
    """
    使用 AkShare 获取股息率（TTM），单位：百分比（如 3.5 表示 3.5%），取不到则返回 NaN。
    """
    df = _get_lg_indicator(symbol)
    if df is None or df.empty:
        return np.nan

    df = df.sort_values("trade_date")
    row = df.iloc[-1]
    # 尝试常见的股息率字段
    for col in ("dv_ttm", "dv_ratio", "dividendyield"):
        val = row.get(col)
        if pd.notna(val):
            try:
                return float(val)
            except Exception:
                continue

    return np.nan


def _parse_yyyymmdd(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m%d")


def _is_new_stock(list_date_yyyymmdd: str, today: datetime, min_days_listed: int) -> bool:
    try:
        list_dt = _parse_yyyymmdd(str(list_date_yyyymmdd))
    except Exception:
        return True
    return (today - list_dt).days < min_days_listed

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Quant stock filter (AkShare)")
    p.add_argument(
        "--style",
        type=str,
        default="momentum",
        choices=["momentum", "growth", "dividend"],
        help="Stock-picking style: momentum (default), growth, dividend",
    )
    p.add_argument(
        "--lookback-days",
        type=int,
        default=90,
        help="Calendar days to fetch daily data (default: 90)",
    )
    p.add_argument(
        "--min-trading-days",
        type=int,
        default=30,
        help="Minimum daily rows required (default: 30)",
    )
    p.add_argument(
        "--min-listed-days",
        type=int,
        default=100,
        help="Exclude stocks listed for fewer than N days (default: 100)",
    )
    p.add_argument("--pe-max", type=float, default=30.0, help="Maximum PE-TTM allowed")
    p.add_argument(
        "--momentum-min",
        type=float,
        default=0.10,
        help="20d return threshold, e.g. 0.10 means +10 percent",
    )
    p.add_argument(
        "--vol-ratio-min",
        type=float,
        default=1.2,
        help="5d average volume / 20d average volume threshold",
    )
    p.add_argument(
        "--near-high-min",
        type=float,
        default=0.90,
        help="close price / 20d high threshold",
    )
    p.add_argument(
        "--min-price",
        type=float,
        default=3.0,
        help="Minimum latest close price to include a stock",
    )
    p.add_argument(
        "--min-amount-avg",
        type=float,
        default=1e7,
        help="Minimum 20d average成交额 (CNY) to include a stock",
    )
    p.add_argument(
        "--max-results",
        type=int,
        default=200,
        help="Maximum number of stocks to keep after filtering (sorted by factors)",
    )
    p.add_argument(
        "--use-cache",
        action="store_true",
        help="Use local cache for stock pool; falls back to cache when live request fails",
    )
    p.add_argument(
        "--stock-pool-cache",
        type=str,
        default="stock_pool_cache.csv",
        help="CSV path for cached stock pool when --use-cache is enabled",
    )
    p.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level: DEBUG, INFO, WARNING, ERROR",
    )
    p.add_argument(
        "--output",
        type=str,
        default="quant_selected_stocks.csv",
        help="Output CSV file path",
    )
    p.add_argument(
        "--div-yield-min",
        type=float,
        default=3.0,
        help="Minimum dividend yield (percent) for dividend style",
    )
    return p


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    _setup_logging(args.log_level)

    style = args.style

    logging.info("开始量化选股...")
    
    # 1. 获取股票池
    stocks = get_stock_pool(use_cache=args.use_cache, cache_path=args.stock_pool_cache)
    logging.info("共获取 %d 只股票", len(stocks))
    
    selected = []
    count = 0
    skipped = {
        "st": 0,
        "new_stock": 0,
        "no_daily": 0,
        "too_few_days": 0,
        "no_factors": 0,
         "too_cheap": 0,
         "illiquid": 0,
        "error": 0,
    }

    today = datetime.today()
    end_date = today.strftime("%Y%m%d")
    start_date = (today - timedelta(days=args.lookback_days)).strftime("%Y%m%d")
    
    for _, row in stocks.iterrows():
        symbol = str(row["symbol"])
        name = str(row["name"])

        # 过滤 ST 股
        if "ST" in name:
            skipped["st"] += 1
            continue

        # 使用上市天数过滤（仅当数据里真的有 list_date 字段时）
        list_date_val = row.get("list_date", None)
        if list_date_val is not None and pd.notna(list_date_val):
            if _is_new_stock(list_date_val, today=today, min_days_listed=args.min_listed_days):
                skipped["new_stock"] += 1
                continue
            
        try:
            # 2. 获取日线数据
            daily = get_daily_data(symbol, start_date=start_date, end_date=end_date)
            if daily is None or daily.empty:
                skipped["no_daily"] += 1
                continue
            if len(daily) < args.min_trading_days:
                skipped["too_few_days"] += 1
                continue

            # 2.1 基础价格与流动性过滤
            try:
                latest_close = float(daily["close"].iloc[-1])
            except Exception:
                latest_close = np.nan

            if np.isfinite(latest_close) and latest_close < args.min_price:
                skipped["too_cheap"] += 1
                continue

            avg_amount_20 = np.nan
            if "amount" in daily.columns:
                avg_amount_20 = daily["amount"].tail(20).astype(float).mean()

            if np.isfinite(avg_amount_20) and avg_amount_20 < args.min_amount_avg:
                skipped["illiquid"] += 1
                continue
                
            # 3. 计算技术因子（20日）
            factors = calculate_factors(daily)
            if not factors:
                skipped["no_factors"] += 1
                continue

            # 衍生 60 日动量和接近 60 日高点
            momentum_60 = np.nan
            price_near_high_60 = np.nan
            try:
                close_series = daily["close"].astype(float)
                if len(close_series) >= 60:
                    momentum_60 = close_series.iloc[-1] / close_series.iloc[-60] - 1
                    high_60 = daily["high"].tail(60).astype(float).max()
                    if high_60 > 0:
                        price_near_high_60 = close_series.iloc[-1] / high_60
            except Exception:
                pass

            # 简单 60 日波动率（供股息风格排序用）
            vol_60 = np.nan
            try:
                ret = daily["close"].astype(float).pct_change().dropna()
                if len(ret) >= 20:
                    vol_60 = ret.tail(60).std()
            except Exception:
                pass

            # 4. 获取基本面因子（PE、股息率）
            pe = get_pe_ratio(symbol)
            div_yield = get_dividend_yield(symbol)

            passes = False
            row_out = {
                "代码": symbol,
                "名称": name,
                "PE": round(pe, 2) if np.isfinite(pe) else "NA",
                "20日涨幅(%)": factors["momentum"] * 100,
                "量比": factors["vol_ratio"],
                "接近20日高点": factors["price_near_high"],
            }

            if np.isfinite(momentum_60):
                row_out["60日涨幅(%)"] = momentum_60 * 100
            if np.isfinite(price_near_high_60):
                row_out["接近60日高点"] = price_near_high_60
            if np.isfinite(div_yield):
                row_out["股息率(%)"] = div_yield
            if np.isfinite(vol_60):
                row_out["波动率60日"] = vol_60

            # 5. 不同风格的筛选逻辑
            if style == "growth":
                # 景气成长：更看重中期动量和接近 60 日高点，估值不过分贵
                if not np.isfinite(momentum_60) or momentum_60 <= 0.15:
                    passes = False
                else:
                    pe_ok = np.isfinite(pe) and 15.0 <= pe <= args.pe_max
                    if (
                        pe_ok
                        and factors["vol_ratio"] > args.vol_ratio_min
                        and (not np.isfinite(price_near_high_60) or price_near_high_60 > 0.8)
                    ):
                        passes = True

            elif style == "dividend":
                # 高股息稳健：高股息、估值不过分高，近期不大跌
                div_ok = np.isfinite(div_yield) and div_yield >= args.div_yield_min
                pe_ok = np.isfinite(pe) and 5.0 <= pe <= args.pe_max
                mom_ok = (
                    factors["momentum"] > -0.05
                    and (not np.isfinite(momentum_60) or momentum_60 > -0.10)
                )
                if div_ok and pe_ok and mom_ok:
                    passes = True

            else:
                # 默认动量风格：原有逻辑
                pe_ok = True
                if np.isfinite(pe):
                    pe_ok = pe < args.pe_max

                if (
                    pe_ok
                    and factors["momentum"] > args.momentum_min
                    and factors["vol_ratio"] > args.vol_ratio_min
                    and factors["price_near_high"] > args.near_high_min
                ):
                    passes = True

            if passes:
                selected.append(row_out)
                logging.info("✅ 选中: %s (%s)", name, symbol)
                
        except Exception as e:
            skipped["error"] += 1
            logging.debug("处理失败: %s (%s)", name, symbol, exc_info=True)
            continue  # 跳过异常股票
        
        count += 1
        if count % 100 == 0:
            logging.info("已处理 %d 只股票...", count)
    
    # 输出结果
    if selected:
        result_df = pd.DataFrame(selected)

        # 按风格选择排序字段
        if style == "growth":
            sort_cols = ["60日涨幅(%)", "20日涨幅(%)", "量比"]
            ascending = [False, False, False]
        elif style == "dividend":
            sort_cols = ["股息率(%)", "PE"]
            ascending = [False, True]
        else:
            # 默认：动量 > 量比 > 接近高点
            sort_cols = ["20日涨幅(%)", "量比", "接近20日高点"]
            ascending = [False, False, False]

        for col in sort_cols:
            if col not in result_df.columns:
                result_df[col] = np.nan
        result_df = result_df.sort_values(
            by=sort_cols,
            ascending=ascending,
        ).reset_index(drop=True)

        # 限制输出数量
        if args.max_results and len(result_df) > args.max_results:
            result_df = result_df.head(args.max_results)

        logging.info("【量化选股结果】前 %d 只：\n%s", len(result_df), result_df.to_string(index=False))
        
        # 保存到 CSV
        result_df.to_csv(args.output, index=False, encoding='utf_8_sig')
        logging.info("结果已保存至 %s", args.output)
    else:
        logging.info("未找到符合条件的股票")

    logging.info("跳过统计: %s", skipped)

if __name__ == "__main__":
    main()