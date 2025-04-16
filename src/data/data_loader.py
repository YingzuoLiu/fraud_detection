"""
数据加载和预处理模块
路径: src/data/data_loader.py
"""
import pandas as pd
import numpy as np
from pathlib import Path
import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
import io
import time
from config.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FraudDataLoader:
    def __init__(self):
        # 配置boto3客户端
        boto_config = BotoConfig(
            retries = dict(
                max_attempts = 3,  # 最大重试次数
                mode = 'adaptive'  # 自适应重试模式
            ),
            connect_timeout = 5,   # 连接超时时间
            read_timeout = 60      # 读取超时时间
        )
        
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
            region_name=Config.AWS_REGION,
            config=boto_config
        )
        
    def load_raw_data(self):
        """加载原始训练和测试数据"""
        try:
            # 读取训练数据
            train_identity = pd.read_csv(
                Path(Config.RAW_DATA_DIR) / 'train_identity.csv'
            )
            train_transaction = pd.read_csv(
                Path(Config.RAW_DATA_DIR) / 'train_transaction.csv'
            )
            
            # 读取测试数据
            test_identity = pd.read_csv(
                Path(Config.RAW_DATA_DIR) / 'test_identity.csv'
            )
            test_transaction = pd.read_csv(
                Path(Config.RAW_DATA_DIR) / 'test_transaction.csv'
            )
            
            # 合并identity和transaction数据
            train_df = pd.merge(
                train_transaction, 
                train_identity, 
                on='TransactionID', 
                how='left'
            )
            
            test_df = pd.merge(
                test_transaction, 
                test_identity, 
                on='TransactionID', 
                how='left'
            )
            
            logger.info(f"Loaded training data shape: {train_df.shape}")
            logger.info(f"Loaded test data shape: {test_df.shape}")
            
            return train_df, test_df
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
            
    def save_to_s3(self, df, key):
        """保存数据到S3，带有重试机制"""
        max_retries = 3
        retry_delay = 1  # 初始延迟1秒
        
        for attempt in range(max_retries):
            try:
                # 将DataFrame转换为CSV格式的字节流
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                
                # 上传到S3
                self.s3.put_object(
                    Bucket=Config.S3_BUCKET,
                    Key=key,
                    Body=csv_buffer.getvalue().encode('utf-8')
                )
                
                logger.info(f"Successfully saved data to S3: {key}")
                return True
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                logger.warning(f"Attempt {attempt + 1} failed with error code {error_code}")
                
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                else:
                    logger.error(f"Failed to save to S3 after {max_retries} attempts")
                    raise
                    
            except Exception as e:
                logger.error(f"Error saving to S3: {e}")
                raise
            
    def load_from_s3(self, key):
        """从S3加载数据"""
        try:
            response = self.s3.get_object(
                Bucket=Config.S3_BUCKET,
                Key=key
            )
            df = pd.read_csv(response['Body'])
            logger.info(f"Successfully loaded data from S3: {key}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading from S3: {e}")
            raise
            
    def get_data_info(self, df):
        """获取数据基本信息"""
        info = {
            "shape": df.shape,
            "columns": df.columns.tolist(),
            "missing_values": df.isnull().sum().to_dict(),
            "data_types": df.dtypes.to_dict(),
            "memory_usage": df.memory_usage(deep=True).sum() / 1024**2,  # MB
        }
        return info

    def basic_clean(self, df):
        """基础数据清洗"""
        # 复制DataFrame避免修改原始数据
        df_clean = df.copy()
        
        # 删除全为空的列
        null_cols = df_clean.columns[df_clean.isnull().all()]
        df_clean.drop(columns=null_cols, inplace=True)
        
        # 处理数值型特征的缺失值
        numeric_cols = df_clean.select_dtypes(include=['int64', 'float64']).columns
        df_clean[numeric_cols] = df_clean[numeric_cols].fillna(df_clean[numeric_cols].mean())
        
        # 处理类别型特征的缺失值
        categorical_cols = df_clean.select_dtypes(include=['object']).columns
        df_clean[categorical_cols] = df_clean[categorical_cols].fillna('Unknown')
        
        return df_clean

    def save_to_local(self, df, filename):
        """保存数据到本地"""
        try:
            save_path = Path(Config.PROCESSED_DATA_DIR) / filename
            df.to_csv(save_path, index=False)
            logger.info(f"Successfully saved data to local: {save_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving to local: {e}")
            raise