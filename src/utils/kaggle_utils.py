"""
Kaggle数据下载工具
路径: src/utils/kaggle_utils.py
"""
import os
import zipfile
import kaggle
from pathlib import Path
import logging
from config.config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_kaggle_credentials():
    """从环境变量设置Kaggle凭证"""
    os.environ['KAGGLE_USERNAME'] = Config.KAGGLE_USERNAME
    os.environ['KAGGLE_KEY'] = Config.KAGGLE_KEY

def download_and_extract_dataset():
    """
    下载并解压IEEE-CIS欺诈检测竞赛数据集
    """
    try:
        # 设置Kaggle凭证
        setup_kaggle_credentials()
        
        # 设置数据保存路径
        save_path = Config.RAW_DATA_DIR
        
        # 确保目录存在
        os.makedirs(save_path, exist_ok=True)
        
        # 下载数据集
        kaggle.api.competition_download_files(
            'ieee-fraud-detection',
            path=save_path,
            quiet=False
        )
        
        logger.info(f"Dataset downloaded successfully to {save_path}")
        
        # 查找并解压ZIP文件
        zip_file = list(Path(save_path).glob('*.zip'))[0]
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(save_path)
        logger.info(f"Dataset extracted successfully")
        
        # 删除ZIP文件
        os.remove(zip_file)
        logger.info(f"Removed ZIP file")
        
        return True
        
    except Exception as e:
        logger.error(f"Error downloading/extracting dataset: {e}")
        return False

if __name__ == "__main__":
    download_and_extract_dataset()