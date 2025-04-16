"""
数据分析代码
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data():
    """加载数据"""
    data_dir = Path('../data/processed')
    train_df = pd.read_csv(data_dir / 'train_clean.csv')
    test_df = pd.read_csv(data_dir / 'test_clean.csv')
    return train_df, test_df

def analyze_basic_info(df):
    """分析基本信息"""
    info = {
        "shape": df.shape,
        "dtypes": df.dtypes.value_counts(),
        "missing_values": df.isnull().sum().sum(),
        "memory_usage": df.memory_usage(deep=True).sum() / 1024**2  # MB
    }
    return info

def analyze_target_distribution(df):
    """分析目标变量分布"""
    if 'isFraud' in df.columns:
        fraud_dist = df['isFraud'].value_counts(normalize=True)
        total_transactions = len(df)
        fraud_transactions = df['isFraud'].sum()
        
        return {
            "distribution": fraud_dist,
            "total_transactions": total_transactions,
            "fraud_transactions": fraud_transactions,
            "fraud_rate": fraud_transactions / total_transactions
        }
    return None

def analyze_feature_types(df):
    """分析特征类型"""
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    categorical_cols = df.select_dtypes(include=['object']).columns
    
    return {
        "numeric_features": list(numeric_cols),
        "categorical_features": list(categorical_cols),
        "n_numeric": len(numeric_cols),
        "n_categorical": len(categorical_cols)
    }

def analyze_time_features(df):
    """分析时间特征"""
    if 'TransactionDT' in df.columns:
        return {
            "start_time": df['TransactionDT'].min(),
            "end_time": df['TransactionDT'].max(),
            "time_span": df['TransactionDT'].max() - df['TransactionDT'].min()
        }
    return None

def analyze_amount_features(df):
    """分析金额相关特征"""
    amount_cols = [col for col in df.columns if 'amt' in col.lower()]
    amount_stats = {}
    
    for col in amount_cols:
        amount_stats[col] = {
            "mean": df[col].mean(),
            "std": df[col].std(),
            "min": df[col].min(),
            "max": df[col].max(),
            "missing": df[col].isnull().sum()
        }
    
    return amount_stats

def main():
    # 加载数据
    logger.info("Loading data...")
    train_df, test_df = load_data()
    
    # 分析基本信息
    logger.info("Analyzing basic information...")
    basic_info = analyze_basic_info(train_df)
    logger.info(f"Data shape: {basic_info['shape']}")
    logger.info(f"Data types distribution:\n{basic_info['dtypes']}")
    logger.info(f"Total missing values: {basic_info['missing_values']}")
    logger.info(f"Memory usage: {basic_info['memory_usage']:.2f} MB")
    
    # 分析目标变量
    logger.info("\nAnalyzing target variable...")
    target_info = analyze_target_distribution(train_df)
    if target_info:
        logger.info(f"Fraud distribution:\n{target_info['distribution']}")
        logger.info(f"Fraud rate: {target_info['fraud_rate']:.4%}")
    
    # 分析特征类型
    logger.info("\nAnalyzing feature types...")
    feature_info = analyze_feature_types(train_df)
    logger.info(f"Number of numeric features: {feature_info['n_numeric']}")
    logger.info(f"Number of categorical features: {feature_info['n_categorical']}")
    
    # 分析时间特征
    logger.info("\nAnalyzing time features...")
    time_info = analyze_time_features(train_df)
    if time_info:
        logger.info(f"Time span: {time_info['time_span']} units")
    
    # 分析金额特征
    logger.info("\nAnalyzing amount features...")
    amount_info = analyze_amount_features(train_df)
    for col, stats in amount_info.items():
        logger.info(f"\n{col} statistics:")
        for stat_name, value in stats.items():
            logger.info(f"{stat_name}: {value}")

if __name__ == "__main__":
    main()