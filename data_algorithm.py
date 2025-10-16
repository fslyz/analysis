import os
import sys
from fpdf import FPDF
from langchain_openai.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from config import DEFAULT_OUTPUT_DIR, MODEL_NAME
from data_reader import read_file_data

# 初始化模型和链
chat = ChatOpenAI(model=MODEL_NAME)
output_parser = StrOutputParser()

# 定义提示词模板
DATA_ANALYSIS_PROMPT = PromptTemplate(
    input_variables=["data_sample"],
    template="""作为资深数据科学家，您需要基于提供的样本数据构建可编程算法规则体系，并提供通用的数据处理方法。输出要求：纯文本技术规范文档格式，禁止使用任何符号标记（▷、→等）。严格按以下四层结构执行：

数据样本：{data_sample}

--- 分析框架指令 ---

一、数据基础解构与特征识别
1. 字段语义解析：通过字段名和样本值推断业务含义和数据类型
2. 场景适配判定：基于字段组合推断可能的业务领域（如金融、医疗、制造、零售等）
3. 质量审计报告：
   - 完整性：评估数据缺失情况及其影响
   - 逻辑合理性：检查字段间约束关系和潜在矛盾
   - 单位一致性：检测数值字段的量纲统一性

二、预处理规则技术规范
1. 清洗操作规程：
   - 异常值处理：基于各字段的数值分布和业务含义确定异常值判断标准（如使用IQR方法或3σ原则）
   - 缺失值方案：针对不同字段的缺失率和数据类型，制定具体的填充策略（如均值/中位数填充、插值法、预测模型填充等）
   - 重复数据处理：基于关键字段组合识别完全或部分重复的记录，并提供去重或合并建议
2. 转换协议：
   - 数值标准化：根据字段的取值范围和分布特点，选择适当的标准化方法（如Z-score标准化、Min-Max归一化、鲁棒缩放等）
   - 分类编码：针对类别型字段的内容特点，提供具体的编码方式（如独热编码、标签编码、目标编码、频率编码等）
   - 时间特征处理：对时间戳字段进行详细的特征工程（如提取年月日、星期、季节、节假日等）
   - 文本数据处理：如包含文本字段，提供分词、向量化等处理建议

三、特征工程数学规则
1. 派生特征构建：
   - 基于数据特点创建有业务意义的新特征
   - 特征交互：探索特征间的潜在关系
   - 聚合特征：如适用，提供分组聚合特征创建方法
2. 特征筛选机制：
   - 相关性分析：使用统计方法评估特征间相关性
   - 重要性评估：提供特征重要性评估方法
   - 降维策略：如需要，提供适当的降维方法

四、模型应用与行业适配
1. 建模方向建议：
   - 基于数据特点推荐合适的建模类型（分类、回归、聚类等）
   - 提供模型选择的基本原则
2. 跨领域通用性：
   - 提供数据处理方法在不同行业场景的通用化建议
   - 强调数据预处理方法的普适性

--- 输出规范 ---
1. 文本格式：纯技术文档表述，禁用任何特殊符号（包括箭头/项目符号等）
2. 代码转换规则说明：对提及的技术方法进行文字化技术描述
3. 专业深度：特征工程需带数学表达式推导过程
4. 拒绝简化：扩展"无需处理"为完整技术说明
5. 字数：800-1000字符四层完整覆盖
"""
)

data_analysis_chain = DATA_ANALYSIS_PROMPT | chat | output_parser

def analyze_dataset(data_sample=None, dataframe=None, max_retries=3):
    """分析数据集并返回结果"""
    # 从DataFrame生成数据样本
    if dataframe is not None and data_sample is None:
        data_sample = dataframe.head(20).to_string(index=True)

    # 尝试获取响应
    for attempt in range(max_retries):
        try:
            return data_analysis_chain.invoke({
                "data_sample": data_sample
            })
        except Exception as e:
            print(f"尝试 {attempt+1}/{max_retries} 失败: {str(e)}")
    return None

def get_available_font():
    """获取系统中可用的中文字体路径（优先选择TTF单一字体）"""
    # 常见的中文字体路径，优先TTF格式
    font_candidates = [
        # Windows系统常见TTF字体
        'C:/Windows/Fonts/simhei.ttf',        # 黑体 (TTF格式)
        'C:/Windows/Fonts/msyh.ttf',          # 微软雅黑 (TTF格式)
        'C:/Windows/Fonts/msyhbd.ttf',        # 微软雅黑粗体 (TTF格式)
        'C:/Windows/Fonts/simkai.ttf',        # 楷体 (TTF格式)
        
        # 最后再尝试TTC字体
        'C:/Windows/Fonts/simsun.ttc',        # 宋体 (TTC格式，备选)
        
        # 程序目录查找
        os.path.join(os.path.dirname(__file__), 'simhei.ttf'),
        os.path.join(os.path.dirname(__file__), 'fonts', 'simhei.ttf')
    ]
    
    # 检查字体文件是否存在
    for font_path in font_candidates:
        if os.path.exists(font_path):
            return font_path
    
    return None

def save_to_pdf(text_content, dataset_name):
    """将分析结果保存为PDF文件，处理中文编码问题"""
    os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)
    
    if not text_content:
        return None
    
    filename = f"{dataset_name}_算法规则.pdf"
    filepath = os.path.join(DEFAULT_OUTPUT_DIR, filename)
    
    # 获取可用字体
    font_path = get_available_font()
    if not font_path:
        print("警告：未找到可用的中文字体，可能导致PDF中文显示异常")
        print("请将中文字体文件（如simhei.ttf）放在程序目录或Windows字体目录下")
    
    # 创建PDF对象
    pdf = FPDF()
    pdf.add_page()
    
    # 添加字体（如果找到）
    font_name = 'Chinese'
    if font_path:
        try:
            # 尝试添加字体
            pdf.add_font(font_name, '', font_path, uni=True)
            pdf.set_font(font_name, size=12)
        except Exception as e:
            print(f"添加字体失败: {str(e)}")
            # 尝试使用fpdf自带的中文字体方案
            try:
                from fpdf import set_global
                set_global("SYSTEM_TTFONTS", os.path.join(os.path.dirname(__file__), "fonts"))
                pdf.add_font('simsun', '', 'simsun.ttc', uni=True)
                pdf.set_font('simsun', size=12)
                font_name = 'simsun'
            except:
                pdf.set_font('Arial', size=12)  #  fallback to default font
    else:
        pdf.set_font('Arial', size=12)  #  fallback to default font
    
    # 处理文本内容（自动换行和分页）
    lines = text_content.split('\n')
    for line in lines:
        if pdf.get_y() > 260:  # 检查页面底部位置
            pdf.add_page()  # 添加新页面
            pdf.set_font(font_name, size=12)
        
        # 处理中文编码问题
        try:
            pdf.multi_cell(0, 10, line)
        except UnicodeEncodeError:
            # 替换无法编码的字符
            line = line.encode('latin-1', errors='replace').decode('latin-1')
            pdf.multi_cell(0, 10, line)
    
    pdf.output(filepath)
    return filepath

if __name__ == "__main__":
    # 获取数据文件
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        dataset_name = sys.argv[2] if len(sys.argv) > 2 else os.path.splitext(os.path.basename(file_path))[0]
        df = read_file_data(file_path)
    else:
        df, dataset_name = read_file_data()

    try:
        # 分析数据并保存结果
        data_sample = df.head(20).to_string(index=False)
        result = analyze_dataset(data_sample=data_sample)

        if result:
            # 保存为PDF
            pdf_path = save_to_pdf(result, dataset_name)
            print(f"PDF分析结果已保存至: {pdf_path}")
    except Exception as e:
        print(f"分析过程中出错: {str(e)}")
    
