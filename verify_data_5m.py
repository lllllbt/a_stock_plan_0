#!/usr/bin/env python3
"""
数据验证脚本
用于验证下载的A股5分钟K线数据的完整性和质量
"""

import os
import pandas as pd
from pathlib import Path


# 配置
DATA_DIR = "data"
KLINE_DIR = "data/kline_5m"
STOCK_LIST_FILE = "data/stock_list.parquet"


def verify_stock_list():
    """验证股票列表"""
    print("=" * 60)
    print("验证股票列表")
    print("=" * 60)

    if not os.path.exists(STOCK_LIST_FILE):
        print(f"错误: 股票列表文件不存在: {STOCK_LIST_FILE}")
        return False

    try:
        stock_list = pd.read_parquet(STOCK_LIST_FILE)
        print(f"总股票数: {len(stock_list)}")
        print(f"数据字段: {stock_list.columns.tolist()}")
        print(f"\n前5只股票:")
        print(stock_list.head())
        return True
    except Exception as e:
        print(f"错误: 无法读取股票列表: {str(e)}")
        return False


def verify_kline_data(symbol: str = "000001"):
    """验证单个股票的K线数据

    Args:
        symbol: 股票代码，默认为 000001（平安银行）
    """
    print("\n" + "=" * 60)
    print(f"验证股票 {symbol} 的K线数据")
    print("=" * 60)

    file_path = os.path.join(KLINE_DIR, f"{symbol}.parquet")

    if not os.path.exists(file_path):
        print(f"错误: 数据文件不存在: {file_path}")
        return False

    try:
        df = pd.read_parquet(file_path)

        print(f"数据行数: {len(df)}")
        print(f"数据字段: {df.columns.tolist()}")
        print(f"时间范围: {df['datetime'].min()} 到 {df['datetime'].max()}")

        print(f"\n前5条数据:")
        print(df.head())

        print(f"\n后5条数据:")
        print(df.tail())

        # 数据质量检查
        print("\n" + "-" * 60)
        print("数据质量检查")
        print("-" * 60)

        # 检查缺失值
        missing_values = df.isnull().sum()
        print(f"\n缺失值统计:")
        print(missing_values)

        # 检查价格数据合理性
        invalid_prices = df[(df['high'] < df['low']) |
                           (df['high'] < df['open']) |
                           (df['high'] < df['close']) |
                           (df['low'] > df['open']) |
                           (df['low'] > df['close'])]

        if len(invalid_prices) > 0:
            print(f"\n警告: 发现 {len(invalid_prices)} 条价格数据不合理的记录")
            print(invalid_prices.head())
        else:
            print("\n✓ 价格数据合理性检查通过")

        # 检查成交量
        negative_volume = df[df['volume'] < 0]
        if len(negative_volume) > 0:
            print(f"\n警告: 发现 {len(negative_volume)} 条负成交量记录")
        else:
            print("✓ 成交量检查通过（无负值）")

        # 验证复权因子
        print("\n" + "-" * 60)
        print("复权因子验证")
        print("-" * 60)

        if 'adj_factor' in df.columns:
            print(f"复权因子范围: {df['adj_factor'].min():.6f} 到 {df['adj_factor'].max():.6f}")
            print(f"复权因子均值: {df['adj_factor'].mean():.6f}")

            # 检查复权因子是否大于0
            invalid_adj = df[df['adj_factor'] <= 0]
            if len(invalid_adj) > 0:
                print(f"\n警告: 发现 {len(invalid_adj)} 条复权因子 <= 0 的记录")
            else:
                print("✓ 复权因子检查通过（全部 > 0）")

            # 计算后复权价格示例
            print("\n后复权价格计算示例（前5条）:")
            df_sample = df.head().copy()
            df_sample['adj_close'] = df_sample['close'] * df_sample['adj_factor']
            df_sample['adj_open'] = df_sample['open'] * df_sample['adj_factor']
            print(df_sample[['datetime', 'close', 'adj_factor', 'adj_close', 'open', 'adj_open']])
        else:
            print("警告: 数据中没有复权因子字段")

        # 计算收益率
        print("\n" + "-" * 60)
        print("收益率统计（基于后复权价格）")
        print("-" * 60)

        if 'adj_factor' in df.columns:
            df['adj_close'] = df['close'] * df['adj_factor']
            df['returns'] = df['adj_close'].pct_change()

            print(f"平均收益率: {df['returns'].mean():.6f}")
            print(f"收益率标准差: {df['returns'].std():.6f}")
            print(f"最大收益率: {df['returns'].max():.6f}")
            print(f"最小收益率: {df['returns'].min():.6f}")

        return True

    except Exception as e:
        print(f"错误: 无法读取或验证数据: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def verify_all_files():
    """验证所有下载的文件"""
    print("\n" + "=" * 60)
    print("验证所有K线数据文件")
    print("=" * 60)

    if not os.path.exists(KLINE_DIR):
        print(f"错误: K线数据目录不存在: {KLINE_DIR}")
        return

    files = list(Path(KLINE_DIR).glob("*.parquet"))
    print(f"找到 {len(files)} 个数据文件")

    if len(files) == 0:
        print("没有找到任何数据文件")
        return

    # 统计信息
    total_records = 0
    file_sizes = []

    for file_path in files:
        try:
            df = pd.read_parquet(file_path)
            total_records += len(df)
            file_sizes.append(os.path.getsize(file_path) / 1024 / 1024)  # MB
        except Exception as e:
            print(f"警告: 无法读取文件 {file_path.name}: {str(e)}")

    print(f"\n总记录数: {total_records:,}")
    print(f"平均每个文件记录数: {total_records / len(files):.0f}")
    print(f"文件大小统计:")
    print(f"  总大小: {sum(file_sizes):.2f} MB")
    print(f"  平均大小: {sum(file_sizes) / len(files):.2f} MB")
    print(f"  最大文件: {max(file_sizes):.2f} MB")
    print(f"  最小文件: {min(file_sizes):.2f} MB")


def main():
    """主函数"""
    print("A股5分钟K线数据验证脚本")
    print("=" * 60)

    # 验证股票列表
    verify_stock_list()

    # 验证单个股票数据（默认000001）
    verify_kline_data("000001")

    # 验证所有文件
    verify_all_files()

    print("\n" + "=" * 60)
    print("验证完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
