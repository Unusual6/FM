import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 设置 Tushare token（免费注册获取：https://tushare.pro）
ts.set_token('8fb3cbe85ce8bce17161fe5c2b756f0efa66dce7a85658204e60d8e3')  # 替换为你的 token
pro = ts.pro_api()

def get_stock_pool():
    """获取当前所有正常交易的A股股票列表"""
    today = datetime.today().strftime('%Y%m%d')
    # 获取股票基本信息
    stock_list = pro.stock_basic(
        exchange='',
        list_status='L',  # L: 上市, D: 退市, P: 暂停
        fields='ts_code,symbol,name,area,industry,list_date'
    )
    return stock_list

def get_daily_data(ts_code, days=60):
    """获取某只股票最近N天的日线数据"""
    end_date = datetime.today().strftime('%Y%m%d')
    start_date = (datetime.today() - timedelta(days=days)).strftime('%Y%m%d')
    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    df = df.sort_values('trade_date').reset_index(drop=True)
    return df

def calculate_factors(df):
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

def get_pe_ratio(ts_code):
    """获取最新市盈率（PE-TTM）"""
    try:
        df = pro.daily_basic(ts_code=ts_code, trade_date='')
        if not df.empty:
            pe = df.iloc[0]['pe_ttm']
            return pe if pd.notna(pe) and pe > 0 else np.inf
    except:
        pass
    return np.inf

def main():
    print("开始量化选股...")
    
    # 1. 获取股票池
    stocks = get_stock_pool()
    print(f"共获取 {len(stocks)} 只股票")
    
    selected = []
    count = 0
    
    for _, row in stocks.iterrows():
        ts_code = row['ts_code']
        symbol = row['symbol']
        name = row['name']
        
        # 过滤ST股、新股（上市<100天）
        if 'ST' in name or int(row['list_date']) > 20250101:
            continue
            
        try:
            # 2. 获取日线数据
            daily = get_daily_data(ts_code, days=60)
            if daily.empty or len(daily) < 30:
                continue
                
            # 3. 计算技术因子
            factors = calculate_factors(daily)
            if not factors:
                continue
                
            # 4. 获取基本面因子（PE）
            pe = get_pe_ratio(ts_code)
            
            # 5. 筛选条件（可调整）：
            # - PE < 30（估值合理）
            # - 20日动量 > 10%
            # - 成交量放大（5日均量 > 20日均量）
            # - 价格接近20日高点（>90%）
            if (
                pe < 30 and
                factors['momentum'] > 0.10 and
                factors['vol_ratio'] > 1.2 and
                factors['price_near_high'] > 0.90
            ):
                selected.append({
                    '代码': symbol,
                    '名称': name,
                    'PE': round(pe, 2),
                    '20日涨幅': f"{factors['momentum']*100:.1f}%",
                    '量比': round(factors['vol_ratio'], 2)
                })
                print(f"✅ 选中: {name} ({symbol})")
                
        except Exception as e:
            continue  # 跳过异常股票
        
        count += 1
        if count % 100 == 0:
            print(f"已处理 {count} 只股票...")
    
    # 输出结果
    if selected:
        result_df = pd.DataFrame(selected)
        print("\n【量化选股结果】")
        print(result_df.to_string(index=False))
        
        # 保存到 CSV
        result_df.to_csv('quant_selected_stocks.csv', index=False, encoding='utf_8_sig')
        print("\n结果已保存至 quant_selected_stocks.csv")
    else:
        print("未找到符合条件的股票")

if __name__ == "__main__":
    main()