from pathlib import Path
import sys

# 添加项目根目录到Python路径
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# 导入配置
from config.config import Config

# 创建必要的目录
Config.make_dirs()