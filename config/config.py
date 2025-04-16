import os
from pathlib import Path
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 获取项目根目录
ROOT_DIR = Path(__file__).resolve().parent.parent

class Config:
    # AWS配置
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
    SAGEMAKER_ROLE = "arn:aws:iam::204529129889:role/SageMaker-ExecutionRole"
    S3_BUCKET = "fraud-detection-project-yz"  # 你需要创建这个bucket
    
    # 路径配置
    DATA_DIR = ROOT_DIR / "data"
    RAW_DATA_DIR = DATA_DIR / "raw"
    PROCESSED_DATA_DIR = DATA_DIR / "processed"
    MODEL_DIR = ROOT_DIR / "models"
    
    # 数据配置
    RANDOM_SEED = 42
    TEST_SIZE = 0.2
    VALIDATION_SIZE = 0.2
    
    # 特征工程配置
    FEATURE_STORE_PATH = DATA_DIR / "feature_store.db"
    
    # 模型配置
    MODEL_PARAMS = {
        "learning_rate": 0.001,
        "batch_size": 64,
        "epochs": 10
    }
    
    # 创建必要的目录
    @staticmethod
    def make_dirs():
        """创建必要的目录结构"""
        dirs = [
            Config.RAW_DATA_DIR,
            Config.PROCESSED_DATA_DIR,
            Config.MODEL_DIR
        ]
        for dir_path in dirs:
            dir_path.mkdir(parents=True, exist_ok=True)