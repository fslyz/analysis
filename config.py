# 配置文件
import os

# API配置
API_KEY = "sk-062b1fc6f6a9425eae59020faa7b5f6e"
API_BASE = "https://api.deepseek.com/v1"

# 模型配置
MODEL_NAME = "deepseek-chat"
MODEL_TEMPERATURE = 0

# 数据路径配置
DEFAULT_OUTPUT_DIR = "F:/analysis/data"

# 数据库配置
DB_TABLE_NAME = "coordinates"
DB_ENGINE = "sqlite:///:memory:"

# 工具配置
MAX_ITERATIONS = 20  # 代理最大迭代次数
VERBOSE = True  # 是否显示详细日志
HANDLE_PARSING_ERRORS = True  # 是否处理解析错误

# 数据清洗配置
MISSING_VALUE_STRATEGY = "auto"  # 默认缺失值处理策略
OUTLIER_METHOD = "iqr"  # 默认异常值检测方法
NORMALIZATION_METHOD = "minmax"  # 默认数据标准化方法

# 导出配置
EXPORT_FORMATS = [".csv", ".xlsx", ".xls"]  # 支持的导出格式
DEFAULT_EXPORT_FORMAT = ".xlsx"  # 默认导出格式

# 初始化环境变量
os.environ["OPENAI_API_KEY"] = API_KEY
os.environ["OPENAI_API_BASE"] = API_BASE
