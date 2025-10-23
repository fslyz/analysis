import os
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
    template="""你是一位数据分析专家，请对以下数据集的前20行进行深入分析，并严格遵循以下要求：

数据集名称：{dataset_name}
数据样本：
{data_sample}

**重要指示：**
1. 你必须分析数据样本，理解字段与数据之间的关系，构建内部数据和外部数据采集情况文本
2. 严格按照指定的俩个文本：分别是内部和外部数据采集情况，输出格式是文本，不得有任何其他文本、解释或格式

**关键约束 - 严格遵守：**
- 仅基于数据样本中实际存在的列创建规则，不得擅自添加任何不存在的字段
- 根据数据样本中的字段类型、值和特征进行深入分析
- 分析必须保持通用性，不针对特定场景
- 不得基于数据内容推断或创建额外的字段
- 输出只能是"内部数据"和"外部数据"两个部分，不得有其他内容

**深入数据分析方法：**
- 识别数据类型与格式特点，分析数据精度与完整性
- 挖掘数据间的关联模式与潜在的层次结构
- 评估数据质量指标（一致性、时效性、准确性）
- 深入推断数据采集系统的架构特点与技术实现
- 识别数据处理流程中的关键技术节点

**内部数据与外部数据区分：**
- 内部数据：系统或组织内部产生、收集和处理的数据
- 外部数据：从系统或组织外部获取的数据，如第三方接口、公开数据源等

**示例 - 仅供参考：**
示例1：
内部数据
运行参数方面，计时模块与里程计数传感器记录总运行时间与里程；速度、角度传感器及 GPS 等获取对应数据。电量由 BMS 采集。部件状态上，各关键部件的心跳监测电路反馈工作状态，特殊功能部件如安全触边等触发传感器在特定状况下发送数据。
外部数据
叉车通过特定接口与生产、仓储管理系统对接获取任务、仓位等信息，借助通信设备接收厂区交通管理系统指令，依此规划路线，在硬件协同下实现高效作业 。
示例2：
内部数据采集
内部数据采集基于自主研发的在线答题系统，聚焦用户选择题交互行为。系统通过事件监听模块实时捕获用户操作（如选项点击、提交动作），记录题号、答案选项、操作时间戳及答题耗时等核心数据。数据处理模块对原始操作数据进行结构化封装（如JSON格式），通过加密传输协议同步至服务器，并存储于关系型数据库。系统内置评分引擎，支持预设规则（如标准答案匹配）或动态权重算法（如时间加权评分）完成实时判分，同时通过异常检测机制（如超时提交、重复操作）过滤无效数据，确保数据完整性与一致性。
外部数据采集
外部数据采集整合多源数据输入与第三方资源。系统支持对接行业题库API，实现题目数据的标准化获取与去重存储。针对非系统内生成的答题数据（如用户上传的电子版试卷），通过模板匹配技术定位选择题区域，结合图像识别算法（如OCR）解析填涂结果，并与用户身份信息自动关联。此外，系统兼容扩展接口，可接入二维码扫描、手势识别等辅助交互方式，完成数据解析与结构化存储，最终实现多模态答题数据的统一管理与分析。

**输出要求：**
1. 输出必须严格分为"内部数据"和"外部数据"两部分
2. 每部分内容需描述数据采集、处理和分析的通用方法
3. 保持描述的通用性，不针对特定数据集
4. 输出只能是"内部数据"和"外部数据"两个部分，不得有其他内容
5. 内部数据和外部数据两部分内容合计不得超过200字
6. 使用专业、通用的数据分析术语，确保内容精炼且有价值

请严格遵循以上所有要求，仅输出内部数据和外部数据两部分内容。"""
)

data_analysis_chain = DATA_ANALYSIS_PROMPT | chat | output_parser

def analyze_dataset(dataset_name, data_sample=None, dataframe=None, max_retries=3):
    """分析数据集并返回结果"""
    print(f"=== {dataset_name} 数据采集分析 ===")
    
    # 从DataFrame生成数据样本
    if dataframe is not None and data_sample is None:
        data_sample = dataframe.head(20).to_string(index=True)
    
    # 尝试获取响应
    for attempt in range(max_retries):
        try:
            return data_analysis_chain.invoke({
                "dataset_name": dataset_name,
                "data_sample": data_sample
            })
        except Exception as e:
            print(f"尝试 {attempt+1}/{max_retries} 失败: {str(e)}")
    return None

def save_to_text(text_content, dataset_name):
    """将分析结果保存为文本文件"""
    os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)
    
    if not text_content:
        return None
    
    filename = f"{dataset_name}_数据采集分析.txt"
    filepath = os.path.join(DEFAULT_OUTPUT_DIR, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(text_content)
    
    return filepath

if __name__ == "__main__":
    import sys
    from data_reader import read_file_data
    
    # 获取数据文件
    if len(sys.argv) > 1:
        file_path = sys.argv
        dataset_name = sys.argv[2] if len(sys.argv) > 2 else os.path.splitext(os.path.basename(file_path))
        df = read_file_data(file_path)
    else:
        df, dataset_name = read_file_data()
    
    try:
        # 分析数据并保存结果
        data_sample = df.head(20).to_string(index=False)
        result = analyze_dataset(dataset_name, data_sample=data_sample)
        
        if result:
            filepath = save_to_text(result, dataset_name)
            print(f"分析结果已保存至: {filepath}")
    except Exception as e:
        print(f"处理错误: {str(e)}")
