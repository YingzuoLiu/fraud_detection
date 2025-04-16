"""
数据处理流水线执行脚本
路径: src/data/run_pipeline.py
"""
import logging
from pathlib import Path
from src.utils.kaggle_utils import download_and_extract_dataset
from src.data.data_loader import FraudDataLoader
from config.config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_data_pipeline():
    """执行完整的数据处理流水线"""
    try:
        # 1. 下载并解压数据集
        logger.info("Downloading and extracting dataset...")
        success = download_and_extract_dataset()
        if not success:
            raise Exception("Failed to download/extract dataset")

        # 2. 初始化数据加载器
        data_loader = FraudDataLoader()

        # 3. 加载原始数据
        logger.info("Loading raw data...")
        train_df, test_df = data_loader.load_raw_data()

        # 4. 获取数据信息
        logger.info("Analyzing data...")
        train_info = data_loader.get_data_info(train_df)
        logger.info(f"Training data info:\n{train_info}")

        # 5. 基础数据清洗
        logger.info("Cleaning data...")
        train_df_clean = data_loader.basic_clean(train_df)
        test_df_clean = data_loader.basic_clean(test_df)

        # 6. 保存处理后的数据
        logger.info("Saving processed data...")
        
        # 首先保存到本地
        data_loader.save_to_local(train_df_clean, 'train_clean.csv')
        data_loader.save_to_local(test_df_clean, 'test_clean.csv')
        logger.info("Saved data to local storage")
        
        try:
            # 尝试保存到S3
            logger.info("Attempting to save to S3...")
            data_loader.save_to_s3(train_df_clean, 'processed/train_clean.csv')
            data_loader.save_to_s3(test_df_clean, 'processed/test_clean.csv')
            logger.info("Successfully saved data to S3")
        except Exception as e:
            logger.warning(f"Failed to save to S3: {e}")
            logger.warning("Continuing with local data only")

        logger.info("Data pipeline completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Error in data pipeline: {e}")
        return False

if __name__ == "__main__":
    run_data_pipeline()