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
DATA_ANALYSIS_PROMPT = PromptTemplate (
    input_variables=["dataset_name","data_sample"],
    template="""作为资深数据科学家, 基于提供的样本数据构建可编程算法规则体系, 提供通用的数据处理方法, 融合数据治理规范与跨行业实战经验输出可落地规则. 输出要求:

数据集名称：{dataset_name}
数据样本：{data_sample}

1. 纯文本技术规范文档格式, 禁止使用任何符号标记
2. 严格控制在300-400字之间
3. 以技术文档风格撰写, 开头直接进入主题, 无需标题或引言
4. 只分析最重要的1-2个字段，不要分析过多不必要字段
5. 必须包含具体技术参数、算法公式、精确数值和量化指标
6. 描述完整的数据处理流程: 数据采集、处理、分析、应用
7. 详细说明动态响应机制, 包括阈值判断、预警系统和自适应调整
8. 提供具体的数值范围、百分比、时间间隔等量化指标
9. 形成监测-分析-决策-执行的闭环控制系统
10. 使用专业术语和技术语言, 保持文档的专业性和技术性
11. 描述应连贯流畅, 段落间有逻辑关联, 形成完整的技术方案
12. 确保内容与示例风格一致, 包含具体的技术实现细节, 如算法公式、阈值设定、响应机制等
13. 文档必须包含具体数值、百分比、时间间隔等精确量化指标
"""
)


data_analysis_chain = DATA_ANALYSIS_PROMPT | chat | output_parser

def analyze_dataset(data_sample=None, dataframe=None, dataset_name=None, max_retries=3):
    """分析数据集并返回结果"""
    # 从DataFrame生成数据样本
    if dataframe is not None and data_sample is None:
        data_sample = dataframe.head(20).to_string(index=True)

    # 尝试获取响应
    for attempt in range(max_retries):
        try:
            return data_analysis_chain.invoke({
                "data_sample": data_sample,
                "dataset_name": dataset_name
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
        result = analyze_dataset(data_sample=data_sample, dataset_name=dataset_name)

        if result:
            # 保存为PDF
            pdf_path = save_to_pdf(result, dataset_name)
            print(f"PDF分析结果已保存至: {pdf_path}")
    except Exception as e:
        print(f"分析过程中出错: {str(e)}")
    
