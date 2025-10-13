import pandas as pd
import os
import chardet
import sqlite3
from tempfile import NamedTemporaryFile
from config import API_KEY, API_BASE, MODEL_NAME

def read_data(file_path):
    """
    读取数据文件（支持.xlsx和.csv格式），返回数据框
    参数:
        file_path: 数据文件的完整路径
    返回:
        pandas DataFrame对象
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"找不到文件: {file_path}")
    
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".xlsx":
        return pd.read_excel(file_path, engine="openpyxl")
    elif ext == ".csv":
        # 自动检测编码并读取CSV
        with open(file_path, 'rb') as f:
            encoding = chardet.detect(f.read(10000))['encoding']
        return pd.read_csv(file_path, encoding=encoding)
    else:
        raise ValueError(f"不支持的文件格式({ext})，请使用.xlsx或.csv文件")

def setup_sql_database(df):
    """将DataFrame加载到临时SQLite数据库并返回连接对象和文件路径"""
    # 创建临时文件作为数据库
    with NamedTemporaryFile(suffix='.db', delete=False) as temp_db_file:
        db_path = temp_db_file.name
    
    # 创建SQLite连接
    conn = sqlite3.connect(db_path)
    df.to_sql("temp_table", conn, index=False, if_exists="replace")
    return conn, db_path

def get_first_rows_directly(conn):
    """直接使用SQL查询获取前20行数据"""
    # 直接执行SQL查询获取前20行数据
    query = "SELECT * FROM temp_table LIMIT 20"
    result_df = pd.read_sql_query(query, conn)
    
    # 将结果转换为美观的表格格式字符串
    return result_df.to_string(index=False)

def clean_up_temp_file(db_path):
    """清理临时数据库文件"""
    try:
        # 尝试多次删除文件，因为Windows有时会锁定文件
        import time
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                if os.path.exists(db_path):
                    os.remove(db_path)
                    return
            except PermissionError:
                if attempt < max_attempts - 1:
                    print(f"删除临时文件失败，等待后重试... (尝试 {attempt + 1}/{max_attempts})")
                    time.sleep(1)  # 等待1秒后重试
                else:
                    print(f"无法删除临时文件: {db_path}")
                    print("请手动删除该文件，或忽略此警告。")
    except Exception as e:
        print(f"清理临时文件时发生错误: {e}")

def read_file_data(file_path=None):
    """
    读取数据文件并返回DataFrame和数据集名称
    
    参数:
        file_path: 数据文件的完整路径，如果为None则通过用户输入获取
    
    返回:
        tuple: (DataFrame, 数据集名称)
    """
    if file_path is None:
        file_path = input("请输入Excel/CSV文件完整路径：").strip()
        
    df = read_data(file_path)
    dataset_name = os.path.splitext(os.path.basename(file_path))[0]
    
    return df, dataset_name

if __name__ == "__main__":
    file_path = input("请输入Excel/CSV文件完整路径：").strip()
    df = read_data(file_path)
    
    # 设置SQL数据库（使用临时文件）
    conn, db_path = setup_sql_database(df)
    
    # 直接使用SQL查询获取前20行数据
    result = get_first_rows_directly(conn)
    print("\n文件前20行内容：")
    print(result)
    
    # 关闭数据库连接
    conn.close()
    
    # 清理临时文件
    clean_up_temp_file(db_path)
