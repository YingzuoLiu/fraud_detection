import json
import boto3
from botocore.exceptions import ClientError
from config.config import Config
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_aws_credentials():
    """验证AWS凭证"""
    try:
        sts = boto3.client(
            'sts',
            aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
            region_name=Config.AWS_REGION
        )
        sts.get_caller_identity()
        return True
    except Exception as e:
        logger.error(f"AWS凭证验证失败: {e}")
        return False

def create_s3_bucket():
    """创建S3存储桶"""
    # 验证凭证
    if not validate_aws_credentials():
        logger.error("AWS凭证无效，请检查配置")
        return False

    s3 = boto3.client(
        's3',
        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
        region_name=Config.AWS_REGION
    )
    
    try:
        # 检查存储桶是否已存在
        try:
            s3.head_bucket(Bucket=Config.S3_BUCKET)
            logger.info(f"Bucket {Config.S3_BUCKET} 已存在")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # 存储桶不存在，创建新的
                logger.info(f"正在创建存储桶: {Config.S3_BUCKET}")
                if Config.AWS_REGION == 'us-east-1':
                    s3.create_bucket(Bucket=Config.S3_BUCKET)
                else:
                    s3.create_bucket(
                        Bucket=Config.S3_BUCKET,
                        CreateBucketConfiguration={
                            'LocationConstraint': Config.AWS_REGION
                        }
                    )
                logger.info(f"存储桶创建成功: {Config.S3_BUCKET}")
            else:
                raise

        # 设置存储桶策略
        bucket_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowSageMakerAccess",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": Config.SAGEMAKER_ROLE
                    },
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{Config.S3_BUCKET}",
                        f"arn:aws:s3:::{Config.S3_BUCKET}/*"
                    ]
                }
            ]
        }
        
        # 应用策略
        s3.put_bucket_policy(
            Bucket=Config.S3_BUCKET,
            Policy=json.dumps(bucket_policy)
        )
        logger.info("存储桶策略设置成功")

        # 设置存储桶加密
        s3.put_bucket_encryption(
            Bucket=Config.S3_BUCKET,
            ServerSideEncryptionConfiguration={
                'Rules': [
                    {
                        'ApplyServerSideEncryptionByDefault': {
                            'SSEAlgorithm': 'AES256'
                        }
                    }
                ]
            }
        )
        logger.info("存储桶加密设置成功")

        return True
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    if create_s3_bucket():
        logger.info("AWS S3设置完成")
    else:
        logger.error("AWS S3设置失败")