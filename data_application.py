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
    template="""作为专业数据分析专家，您需基于数据集前20行执行跨行业适配性深度分析。核心目标：精准推断数据应用场景并定位适配行业，严格遵循以下分析框架：

数据集名称：{dataset_name}
数据样本：
{data_sample}

## 分析要求
1. **基础数据解析**  
   - 字段构成：明确列名、数据类型（数值/文本/日期/枚举/地理信息等）  
   - 字段关联：识别字段间逻辑关系（如主键-外键、时空序列、分类层级）  
   - 数据特征：标注取值范围、格式规律、空值率、更新频率等关键属性  

2. **跨行业场景分析**（按五部分结构化输出）  
   - 行业属性定位：适配的1-3个核心行业及可拓展关联行业，标注适配依据（如“设备ID+运行参数”适配工业监控）  
   - 场景核心定义：基于行业需求定义具体场景（例：“能源行业-发电设备状态监测场景”）  
   - 场景需求拆解：拆解效率提升/风险控制/成本优化等需求，说明数据与需求的对应关系  
   - 数据支撑逻辑：解释字段组合如何协同支撑场景（例：“坐标+尺寸参数”支撑定日镜场布局优化）  
   - 跨行业适配边界：说明可迁移特征（如时序数据）与限制条件（如缺失用户画像字段限制精准营销）  

## 关键约束（违反将导致分析失效）
- 行业定位：必须标注“核心适配场景”（数据完全匹配）与“部分适配场景”（需补充≤2个字段）  
- 推断依据：仅使用数据样本中实际存在的字段，禁止添加虚构字段或无数据支撑的假设  
- 范围聚焦：仅分析“数据特征-行业需求”的匹配逻辑，禁止延伸行业属性或业务背景  
- 输出规范：禁用**等特殊符号，标题分行不加粗（例：行业属性定位→行业属性定位）  

## 分析方法论（供推理参考）
```markdown
1. 字段-行业映射：通过特征字段匹配行业共性需求（如“患者ID+诊断代码”→医疗）
2. 数据交互-场景关联：分析字段组合与场景流程契合度（如“设备参数+故障记录”→工业运维）
3. 跨行业兼容性评估：判断特征可迁移性（如“时序监测”适配工业监控与金融风控）
输出要求
严格按五部分顺序输出：
行业属性定位
场景核心定义
场景需求拆解
数据支撑逻辑
跨行业适配边界
每部分内容：
基于数据特征明确标注判断依据（例：“因含‘商品编码+交易时间’，适配零售销售分析”）
多行业适配时逐一分析不得遗漏
使用跨行业通用术语（如避免“FDA认证”等专属词汇）
文本规范：
纯文本结构化分行，无标题符号
每部分限200字以内
禁用表格/项目符号等非文本格式
""" )

data_analysis_chain = DATA_ANALYSIS_PROMPT | chat | output_parser

def analyze_dataset(dataset_name, data_sample=None, dataframe=None, max_retries=3):
    """分析数据集并返回结果"""
    print(f"=== {dataset_name} 应用场景分析 ===")

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

    filename = f"{dataset_name}_数据应用场景分析.txt"
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
        print(f"分析过程中出错: {str(e)}")
