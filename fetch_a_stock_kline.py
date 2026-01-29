#!/usr/bin/env python3
"""
A股日线K线数据获取脚本
获取所有A股的日线K线数据（不复权 + 复权因子），保存为Parquet格式
"""

import os
import sys
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict

import akshare as ak
import pandas as pd
from tqdm import tqdm


# ==================== 配置模块 ====================

DATA_DIR = "data"
KLINE_DIR = "data/kline_daily"
STOCK_LIST_FILE = "data/stock_list.parquet"
DEFAULT_START_DATE = "19910101"  # 日线数据可以追溯到1991年
MAX_RETRIES = 3
REQUEST_DELAY = 0.5  # 请求间隔（秒）


# ==================== 工具函数 ====================

def ensure_directories():
    """确保数据目录存在"""
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    Path(KLINE_DIR).mkdir(parents=True, exist_ok=True)
    print(f"数据目录已创建: {DATA_DIR}, {KLINE_DIR}")


def retry_on_error(func, max_retries=MAX_RETRIES, delay=REQUEST_DELAY):
    """错误重试装饰器"""
    def wrapper(*args, **kwargs):
        for attempt in range(max_retries):
            try:
                result = func(*args, **kwargs)
                time.sleep(delay)  # 请求间隔
                return result
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                print(f"  尝试 {attempt + 1}/{max_retries} 失败: {str(e)}, 重试中...")
                time.sleep(delay * (attempt + 1))  # 递增延迟
        return None
    return wrapper


# ==================== 股票列表获取模块 ====================

def get_stock_list() -> pd.DataFrame:
    """获取所有A股股票代码列表"""
    print("正在获取A股股票列表...")

    @retry_on_error
    def fetch():
        return ak.stock_zh_a_spot_em()

    df = fetch()
    if df is None or df.empty:
        raise ValueError("无法获取股票列表")

    # 提取股票代码和名称
    stock_list = df[['代码', '名称']].copy()
    stock_list.columns = ['symbol', 'name']

    # 保存到文件
    stock_list.to_parquet(STOCK_LIST_FILE, index=False)
    print(f"股票列表已保存: {STOCK_LIST_FILE}, 共 {len(stock_list)} 只股票")

    return stock_list


# ==================== 复权因子获取模块 ====================

def fetch_adj_factor(symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """获取复权因子

    Args:
        symbol: 股票代码
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)

    Returns:
        包含日期和复权因子的DataFrame，列名为 ['date', 'adj_factor']
    """
    @retry_on_error
    def fetch():
        # 获取前复权和不复权数据来计算复权因子
        df_qfq = ak.stock_zh_a_hist(symbol=symbol, period="daily",
                                     start_date=start_date, end_date=end_date,
                                     adjust="qfq")  # 前复权
        df_raw = ak.stock_zh_a_hist(symbol=symbol, period="daily",
                                     start_date=start_date, end_date=end_date,
                                     adjust="")  # 不复权

        if df_qfq is None or df_raw is None or df_qfq.empty or df_raw.empty:
            return None

        # 计算复权因子 = 前复权价格 / 不复权价格
        df_adj = pd.DataFrame({
            'date': pd.to_datetime(df_raw['日期']),
            'adj_factor': df_qfq['收盘'].values / df_raw['收盘'].values
        })

        return df_adj

    return fetch()


# ==================== K线数据获取模块 ====================

def fetch_kline_daily(symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """获取单个股票的日线K线数据（不复权 + 复权因子）

    Args:
        symbol: 股票代码
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)

    Returns:
        包含日线K线数据和复权因子的DataFrame
    """
    @retry_on_error
    def fetch_daily_data():
        # 获取不复权日线数据
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=""  # 不复权
        )
        return df

    # 获取日线数据
    df_kline = fetch_daily_data()
    if df_kline is None or df_kline.empty:
        return None

    # 获取复权因子
    df_adj = fetch_adj_factor(symbol, start_date, end_date)
    if df_adj is None or df_adj.empty:
        print(f"  警告: 无法获取复权因子，使用默认值1.0")
        df_adj = pd.DataFrame({
            'date': pd.to_datetime(df_kline['日期']),
            'adj_factor': 1.0
        })

    # 标准化列名
    df_kline = df_kline[['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额']].copy()
    df_kline.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
    df_kline['date'] = pd.to_datetime(df_kline['date'])

    # 合并复权因子（按日期）
    df_result = df_kline.merge(df_adj[['date', 'adj_factor']], on='date', how='left')

    # 填充缺失的复权因子（使用前向填充）
    df_result['adj_factor'] = df_result['adj_factor'].ffill()
    df_result['adj_factor'] = df_result['adj_factor'].fillna(1.0)

    # 添加股票代码
    df_result['symbol'] = symbol

    # 选择最终列
    df_result = df_result[['date', 'symbol', 'open', 'high', 'low', 'close',
                           'volume', 'amount', 'adj_factor']]

    return df_result


# ==================== 数据保存模块 ====================

def save_to_parquet(df: pd.DataFrame, symbol: str, mode: str = 'overwrite'):
    """保存数据到Parquet文件

    Args:
        df: 数据DataFrame
        symbol: 股票代码
        mode: 保存模式 ('overwrite' 或 'append')
    """
    file_path = os.path.join(KLINE_DIR, f"{symbol}.parquet")

    if mode == 'append' and os.path.exists(file_path):
        # 增量更新模式
        df_existing = pd.read_parquet(file_path)
        df_combined = pd.concat([df_existing, df], ignore_index=True)
        df_combined.drop_duplicates(subset=['date'], keep='last', inplace=True)
        df_combined.sort_values('date', inplace=True)
        df_combined.to_parquet(file_path, index=False)
    else:
        # 覆盖模式
        df.to_parquet(file_path, index=False)


def check_existing_data(symbol: str) -> Optional[str]:
    """检查已存在的数据，返回最新日期

    Args:
        symbol: 股票代码

    Returns:
        最新日期字符串 (YYYYMMDD) 或 None
    """
    file_path = os.path.join(KLINE_DIR, f"{symbol}.parquet")
    if not os.path.exists(file_path):
        return None

    try:
        df = pd.read_parquet(file_path)
        if df.empty:
            return None
        latest_date = df['date'].max()
        return latest_date.strftime('%Y%m%d')
    except Exception:
        return None


# ==================== 主流程模块 ====================

def main():
    """主流程"""
    parser = argparse.ArgumentParser(description='A股日线K线数据获取脚本')
    parser.add_argument('--start-date', type=str, default=DEFAULT_START_DATE,
                        help=f'开始日期 (YYYYMMDD), 默认: {DEFAULT_START_DATE}')
    parser.add_argument('--end-date', type=str, default=None,
                        help='结束日期 (YYYYMMDD), 默认: 今天')
    parser.add_argument('--symbols', type=str, default=None,
                        help='指定股票代码，多个用逗号分隔，例如: 000001,000002')
    parser.add_argument('--update', action='store_true',
                        help='增量更新模式（只下载新数据）')
    parser.add_argument('--limit', type=int, default=None,
                        help='限制下载股票数量（用于测试）')

    args = parser.parse_args()

    # 设置结束日期
    if args.end_date is None:
        args.end_date = datetime.now().strftime('%Y%m%d')

    print("=" * 60)
    print("A股日线K线数据获取脚本")
    print("=" * 60)
    print(f"开始日期: {args.start_date}")
    print(f"结束日期: {args.end_date}")
    print(f"更新模式: {'是' if args.update else '否'}")
    print("=" * 60)

    # 确保目录存在
    ensure_directories()

    # 获取股票列表
    if args.symbols:
        # 使用指定的股票代码
        symbols = [s.strip() for s in args.symbols.split(',')]
        stock_list = pd.DataFrame({'symbol': symbols, 'name': [''] * len(symbols)})
        print(f"使用指定股票: {symbols}")
    else:
        # 获取所有A股
        stock_list = get_stock_list()

    # 限制数量（用于测试）
    if args.limit:
        stock_list = stock_list.head(args.limit)
        print(f"限制下载数量: {args.limit}")

    # 统计信息
    total_stocks = len(stock_list)
    success_count = 0
    failed_stocks = []

    print(f"\n开始下载 {total_stocks} 只股票的日线K线数据...")
    print("=" * 60)

    # 遍历每个股票
    for idx, row in tqdm(stock_list.iterrows(), total=total_stocks, desc="下载进度"):
        symbol = row['symbol']
        name = row.get('name', '')

        try:
            # 增量更新模式：检查已有数据
            start_date = args.start_date
            if args.update:
                latest_date = check_existing_data(symbol)
                if latest_date:
                    # 从最新日期的下一天开始
                    start_date = (datetime.strptime(latest_date, '%Y%m%d') +
                                  timedelta(days=1)).strftime('%Y%m%d')
                    if start_date > args.end_date:
                        print(f"[{symbol}] {name} - 数据已是最新，跳过")
                        success_count += 1
                        continue

            # 获取K线数据
            df = fetch_kline_daily(symbol, start_date, args.end_date)

            if df is None or df.empty:
                print(f"[{symbol}] {name} - 无数据")
                failed_stocks.append((symbol, name, "无数据"))
                continue

            # 保存数据
            save_mode = 'append' if args.update else 'overwrite'
            save_to_parquet(df, symbol, mode=save_mode)

            success_count += 1
            print(f"[{symbol}] {name} - 成功 ({len(df)} 条记录)")

        except Exception as e:
            print(f"[{symbol}] {name} - 失败: {str(e)}")
            failed_stocks.append((symbol, name, str(e)))

    # 生成下载报告
    print("\n" + "=" * 60)
    print("下载完成报告")
    print("=" * 60)
    print(f"总股票数: {total_stocks}")
    print(f"成功: {success_count}")
    print(f"失败: {len(failed_stocks)}")

    if failed_stocks:
        print("\n失败股票列表:")
        for symbol, name, error in failed_stocks[:10]:  # 只显示前10个
            print(f"  [{symbol}] {name} - {error}")
        if len(failed_stocks) > 10:
            print(f"  ... 还有 {len(failed_stocks) - 10} 只股票失败")

    print("\n数据保存位置:")
    print(f"  股票列表: {STOCK_LIST_FILE}")
    print(f"  K线数据: {KLINE_DIR}/")
    print("=" * 60)


if __name__ == "__main__":
    main()
