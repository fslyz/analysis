import os
import json
import pandas as pd
from langchain_openai.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from config import DEFAULT_OUTPUT_DIR, MODEL_NAME

# 初始化模型和链
chat = ChatOpenAI(model=MODEL_NAME)
output_parser = StrOutputParser()

# 定义提示词模板
DATA_ANALYSIS_PROMPT = PromptTemplate(
    input_variables=["data_sample", "dataset_name"],
    template="""你是一位数据分析专家，请对以下数据集的前20行进行分析，并严格遵循以下要求：

数据集名称：{dataset_name}
数据样本：
{data_sample}

**重要指示：**
1. 你必须分析数据样本，理解字段与数据之间的关系，构建数据字典
2. 严格按照指定的六个字段输出：字段中文名，字段英文名，字段类型，字段长度，样例数据，是否主键
3. 输出格式必须且只能是JSON，不得有任何其他文本、解释或格式
4. JSON结构必须为：{{"algorithm_rules": [{{"字段中文名":,"字段英文名":"","字段类型":"","字段长度":"","样例数据":"","是否主键":""}},...]}}规则":""}},...]}}
5. 样例数据必须从实际数据中提取，不得编造

**关键约束 - 严格遵守：**
- 仅基于数据样本中实际存在的列创建规则，不得擅自添加任何不存在的字段
- 数据样本中有几个字段，算法规则表中就必须有几个条目，不多不少
- 每个条目必须直接对应数据样本中的一个列
- 不得基于数据内容推断或创建额外的字段
- 每个字段的所有属性都必须填写完整，不得留空
- 样例数据必须来自实际数据样本，不得编造或虚构

**字段规范与约束：**
1. 字段中文名: 为每个字段提供专业、清晰的中文名称, 反映字段的业务含义
   - 如果字段名已经是中文, 请直接使用该中文作为字段中文名
   - 如果字段名是英文, 请将其翻译为贴切的中文作为字段中文名
   - 确保中文名称准确反映字段用途，不得任意添加或更改业务含义
2. 字段英文名: 使用英文命名, 遵循驼峰命名法或下划线命名法
   - 如果字段名已经是英文, 请直接使用该英文作为字段英文名
   - 如果字段名是中文, 请将其翻译为贴切的英文作为字段英文名
   - 英文名称必须与原始字段名含义一致，不得随意更改
3. 字段类型: 从以下预定义类型中选择最合适的:
   - INT: 整数类型
   - BIGINT: 大整数类型
   - TINYINT: 微小整数类型
   - SMALLINT: 小整数类型
   - FLOAT: 单精度浮点数
   - DOUBLE: 双精度浮点数
   - STRING: 字符串类型
   - CHAR: 定长字符类型
   - VARCHAR: 变长字符类型
   - TEXT: 长文本类型
   - DATE: 日期类型
   - TIMESTAMP: 时间戳类型
   - 基于实际数据内容精确判断，不得随意选择
4. 字段长度:
   - 所有字段类型都必须填写一个合适的长度值，不得留空或填写"-"
   - 对于字符类型(CHAR, VARCHAR, TEXT)，根据实际数据长度指定合适的长度，考虑未来可能的扩展
   - 对于整数类型(INT, BIGINT, TINYINT, SMALLINT)，填写该类型的标准位数(如INT:11, BIGINT:20, TINYINT:4, SMALLINT:6)
   - 对于浮点数类型(FLOAT, DOUBLE)，填写总位数和小数位数(如FLOAT:10,2，DOUBLE:20,5)
   - 对于日期类型(DATE)，填写固定长度10(YYYY-MM-DD格式)
   - 对于时间戳类型(TIMESTAMP)，填写固定长度19(YYYY-MM-DD HH:MM:SS格式)
   - 长度值必须基于实际数据样本和字段类型特性综合确定
5. 是否主键:
   - 基于字段名称和内容判断是否可能为主键
   - 只有两个选项: "是" 或 "否"
   - 每个表必须且只能有一个字段标记为主键
   - 如果没有明显的主键字段，请选择最合适的唯一标识字段作为主键
   - 如无法确定，选择第一个字段作为主键
6. 样例数据:
   - 必须从实际数据样本中提取1个有代表性的数据样例
   - 不得编造或虚构任何样例数据
   - 样例数据必须来自数据样本中的实际数据
   - 选择能体现字段特征的典型数据作为样例

**示例参考：**
示例1：
中文名称 | 英文名称 | 数据类型 | 字段长度 | 是否主键
项目编号 | Project Number | VARCHAR | 10 | 是
施工段编号 | Construction section number | VARCHAR | 15 | 否
施工段名称 | Construction section name | VARCHAR | 50 | 否
施工描述 | Construction Description | VARCHAR | 200 | 否
划分依据 | Division based on | VARCHAR | 50 | 否

示例2：
中文名称 | 英文名称 | 数据类型 | 字段长度 | 举例示例 | 是否主键
项目编号 | Item Number | VARCHAR | 10 | Z001 | 否
节点编号 | Node Number | VARCHAR | 15 | Z001-1 | 是
关键节点名称 | Key Node Name | VARCHAR | 100 | 视频前端监控点 | 否
节点类型 | Node Type | VARCHAR | 20 | 物理节点 | 否

**输出要求：**
1. 仅输出有效的JSON格式，前后不得添加任何解释文字
2. JSON必须可直接解析，无格式错误
3. 所有字段必须存在且非空
4. 中文字符必须使用UTF-8编码
5. 算法规则表中的条目数量必须与数据样本中的列数完全一致
6. 每个字段的属性必须完整且准确
7. 样例数据必须来自实际数据样本

请严格遵循以上所有要求，仅输出JSON格式的算法规则表。"""
)

data_analysis_chain = DATA_ANALYSIS_PROMPT | chat | output_parser

def analyze_dataset(dataset_name, data_sample=None, dataframe=None, max_retries=3):
    """分析数据集并返回JSON结果"""
    print(f"=== {dataset_name} 数据字典分析 ===")
    
    # 从DataFrame生成数据样本
    if dataframe is not None and data_sample is None:
        data_sample = dataframe.head(20).to_string(index=True)
    
    # 尝试获取有效响应
    for attempt in range(max_retries):
        try:
            result = data_analysis_chain.invoke({
                "dataset_name": dataset_name,
                "data_sample": data_sample
            })
            # 尝试提取JSON
            json_data = extract_json(result)
            if json_data:
                return json_data
            print(f"尝试 {attempt+1}/{max_retries}: 未找到有效JSON")
        except Exception as e:
            print(f"尝试 {attempt+1}/{max_retries}: 发生错误 - {str(e)}")
    return None
def extract_json(text):
    """从文本中提取JSON对象"""
    try:
        # 尝试直接解析
        return json.loads(text)
    except json.JSONDecodeError:
        # 查找JSON结构
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1:
            try:
                return json.loads(text[start:end+1])
            except json.JSONDecodeError:
                pass
    return None

def save_to_excel(json_data, dataset_name):
    """将JSON数据保存为Excel文件"""
    os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)
    
    if not json_data:
        print("无有效数据可保存")
        return None
    
    # 提取字典列表
    rules = json_data.get("algorithm_rules", [])
    if not rules:
        print("未找到数据字典")
        return None
    
    # 创建DataFrame并保存
    df = pd.DataFrame(rules)
    filename = f"{dataset_name}_数据字典.xlsx"
    filepath = os.path.join(DEFAULT_OUTPUT_DIR, filename)
    df.to_excel(filepath, index=False)
    
    print(f"数据字典已保存至: {filepath}")
    return filepath

if __name__ == "__main__":
    import sys
    from data_reader import read_file_data
    
    # 获取数据文件
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        dataset_name = sys.argv[2] if len(sys.argv) > 2 else os.path.splitext(os.path.basename(file_path))[0]
        df = read_file_data(file_path)[0]  # 只取DataFrame
    else:
        # 通过data_reader的交互式功能获取文件路径和数据
        df, dataset_name = read_file_data()
    
    try:
        # 分析数据并保存结果
        data_sample = df.head(20).to_string(index=False)
        result = analyze_dataset(dataset_name, data_sample=data_sample)
        if result:
            save_to_excel(result, dataset_name)
        else:
            print("未能获取有效分析结果")
    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")
