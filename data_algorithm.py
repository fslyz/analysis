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
1. 你必须分析数据样本，理解字段与数据之间的关系，构建算法规则表
2. 严格按照指定的四个字段输出：序号、中文名称、描述、数据规则
3. 输出格式必须且只能是JSON，不得有任何其他文本、解释或格式
4. JSON结构必须为：{{"algorithm_rules": [{{"序号":1,"中文名称":"","描述":"","数据规则":""}},...]}}

**关键约束 - 严格遵守：**
- 仅基于数据样本中实际存在的列创建规则，不得擅自添加任何不存在的字段
- 数据样本中有几个字段，算法规则表中就必须有几个条目，不多不少
- 每个条目必须直接对应数据样本中的一个列
- 不得基于数据内容推断或创建额外的字段

**字段规范与约束：**
- 序号：必须是连续的正整数，从1开始，不得重复或跳号
- 中文名称：简洁明确，不超过15个汉字，使用标准术语
- 描述：简明扼要说明字段用途，不超过30个汉字
- 数据规则：详细说明数据格式、取值范围、计算逻辑或特殊约束，使用技术术语

**示例参考：**
序号 | 中文名称 | 描述 | 数据规则
1 | 问题流水号 | 自增主键 | BIGINT自增，范围1-184亿亿
2 | 事件编号 | 问题唯一业务标识 | 区域代码(420115)+日期(YYMMDD)+4位序列码
3 | 发现时间 | 问题首次上报时间 | 精确到秒的UTC时间戳
4 | 更新时间 | 最后修改时间 | 自动更新触发器维护

序号 | 中文名称 | 描述 | 数据规则
6 | 题型 | 题目类型编码 | 限定枚举值：RPCT（判断题）/MCQ（单选题）/MAQ（多选题）
7 | 用户答案 | 用户提交的选项 | 存储格式为"选项编码:选项文本"，如B:错；多选题用逗号分隔
8 | 正确答案 | 系统预设正确答案 | 与用户答案比对时忽略文本部分，仅校验选项编码是否完全匹配
9 | 实际得分 | 用户本题获得分数 | 计算公式：if(UserAnswerCode == CorrectAnswerCode) then TotalScore else 0

**输出要求：**
1. 仅输出有效的JSON格式，前后不得添加任何解释文字
2. JSON必须可直接解析，无格式错误
3. 所有字段必须存在且非空
4. 中文字符必须使用UTF-8编码
5. 算法规则表中的条目数量必须与数据样本中的列数完全一致

请严格遵循以上所有要求，仅输出JSON格式的算法规则表。"""
)

data_analysis_chain = DATA_ANALYSIS_PROMPT | chat | output_parser

def analyze_dataset(dataset_name, data_sample=None, dataframe=None, max_retries=3):
    """分析数据集并返回JSON结果"""
    print(f"=== {dataset_name} 算法规则表分析 ===")
    
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
        print("未找到算法规则表")
        return None
    
    # 创建DataFrame并保存
    df = pd.DataFrame(rules)
    filename = f"{dataset_name}_算法规则表.xlsx"
    filepath = os.path.join(DEFAULT_OUTPUT_DIR, filename)
    df.to_excel(filepath, index=False)
    
    print(f"算法规则表已保存至: {filepath}")
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
