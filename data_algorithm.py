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
input_variables=["data_sample"],
template=""" 作为资深数据科学家，您需要基于提供的样本数据构建可编程算法规则体系，并提供通用的数据处理方法，同时融合《数据治理国家标准（GB/T 35273-2020）》《数据质量评价指标》等规范要求，结合跨行业实战经验输出可落地规则。输出要求：纯文本技术规范文档格式，禁止使用任何符号标记（▷、→等）。严格按以下四层结构执行：
数据样本：{data_sample}
--- 分析框架指令 ---
一、数据基础解构与特征识别
字段语义解析：通过字段名和样本值推断业务含义和数据类型，按 “字段名 - 样本值 - 业务含义 - 类型 - 判断依据” 逻辑拆解，示例：若字段名为 “用户年龄”、样本值为 “25,32,41”，则业务含义为 “用户生理年龄”，数据类型为 “数值型（连续）”，判断依据为 “样本值为无固定类别、可连续取值的数字，符合数值型字段定义”。
场景适配判定：基于字段组合推断可能的业务领域（如金融、医疗、制造、零售等），结合字段与领域的关联逻辑说明，示例：若含 “贷款金额”“逾期天数”“信用评分” 字段，可推断为金融领域，因该领域信贷风控场景需通过资金相关字段评估用户还款能力与风险等级。
质量审计报告：
完整性：评估数据缺失情况及其影响，统计各字段缺失值数量与缺失率，说明不同缺失率影响，如 “缺失率 < 5%：对建模影响较小，数据代表性仍满足基础分析需求；缺失率≥30%：特征信息严重不足，需补充数据或剔除该字段”，参考《数据质量评价指标》完整性要求。
逻辑合理性：检查字段间约束关系和潜在矛盾，先列出行业专属约束规则（如金融 “贷款金额> 0”、医疗 “诊断日期≥入院日期”），再核查样本是否符合，若存在矛盾标注位置及修正方向，示例：“若第 5 条记录‘贷款金额 =-1000’，违反‘贷款金额 > 0’规则，修正方向为将负数替换为正数，或确认数据录入错误后删除该记录”。
单位一致性：检测数值字段的量纲统一性，核查数值字段量纲是否一致，示例：若 “商品重量” 字段样本值为 “5kg,3000g,2.5kg”，存在 kg 与 g 两种单位，不一致样本为 “3000g”，需统一转换为 kg（3000g=3kg），确保量纲统一。
二、预处理规则技术规范
清洗操作规程：
异常值处理：基于各字段的数值分布和业务含义确定异常值判断标准（如使用 IQR 方法或 3σ 原则），说明行业适配的方法选择逻辑，示例：零售行业 “客单价” 字段含极端高值（如 10000 元，远超普通消费 200-500 元范围），选用 IQR 方法，步骤为：计算四分位数 Q1（如下四分位数 220）、Q3（如上四分位数 480），计算 IQR=Q3-Q1=260，确定异常值范围 “<Q1-1.5IQR（220-390=-170）或 > Q3+1.5IQR（480+390=870）”，标注 “10000 元” 为异常值，处理方式为用 Q3+1.5IQR（870）替换。
缺失值方案：针对不同字段的缺失率和数据类型，制定具体的填充策略（如均值 / 中位数填充、插值法、预测模型填充等），按 “缺失率 - 字段类型 - 处理方法 - 选择理由” 逻辑说明，示例：“用户学历” 字段缺失率 12%（分类型），采用众数填充（众数为 “本科”），选择理由为 “分类型字段无均值 / 中位数，缺失率 < 30% 时众数能保留字段主要分布特征，且操作简单易落地”。
重复数据处理：基于关键字段组合识别完全或部分重复的记录，并提供去重或合并建议，先定义行业关键唯一标识（如金融 “客户 ID + 交易流水号”、零售 “订单 ID + 商品 ID”），再说明处理方式，示例：零售行业关键标识为 “订单 ID + 商品 ID”，完全重复（所有字段值一致）时直接删除重复记录；部分重复（关键标识一致，“购买数量” 分别为 2 和 3）时，合并规则为 “取购买数量最大值，或确认数据后保留最新记录”。
转换协议：
数值标准化：根据字段的取值范围和分布特点，选择适当的标准化方法（如 Z-score 标准化、Min-Max 归一化、鲁棒缩放等），按 “字段特性 - 方法 - 公式 - 参数含义 - 示例” 说明，示例：“用户消费金额”（取值范围 10-5000，无明显极端值），选用 Min-Max 归一化，公式为 x'=(x-min)/(max-min)，x 为原始值，min 为字段最小值（10），max 为字段最大值（5000），如原始值 “200”，计算得 x'=(200-10)/(5000-10)=190/4990≈0.038。
分类编码：针对类别型字段的内容特点，提供具体的编码方式（如独热编码、标签编码、目标编码、频率编码等），按 “类别数量 - 是否有序 - 编码方法 - 示例” 说明，示例：“用户会员等级”（类别：普通、银卡、金卡，有序），采用有序标签编码，赋值 “普通 = 1，银卡 = 2，金卡 = 3”，因有序类别需保留等级递进关系，独热编码会丢失该逻辑。
时间特征处理：对时间戳字段进行详细的特征工程（如提取年月日、星期、季节、节假日等），补充行业专属时间维度，示例：零售行业时间戳 “2024-06-18”，提取特征为年 = 2024，月 = 6，日 = 18，星期 = 星期二，季节 = 夏季，行业专属特征 = 618 促销季（因 6 月 18 日为零售行业重要促销节点）。
文本数据处理：如包含文本字段，提供分词、向量化等处理建议，若为行业专属文本（如医疗 “诊断描述”），需补充行业词典，示例：医疗 “诊断描述” 文本 “患者出现咳嗽、发烧，伴胸闷”，处理步骤为：用医学专用分词工具（如 jieba 医疗分词）分词 “患者 / 出现 / 咳嗽 / 发烧 / 伴 / 胸闷”，加载医学停用词表（剔除 “出现 / 伴”），采用 TF-IDF 向量化，计算 TF（“咳嗽” 在该文本中出现 1 次，文本总词数 4，TF=1/4=0.25）、IDF（总文档数 100，含 “咳嗽” 的文档 30，IDF=log (100/30)≈1.203），TF-IDF=0.25×1.203≈0.301；若样本无文本字段，需说明 “当前样本不包含文本字段，无需执行分词、向量化操作，后续新增文本字段可参考对应行业处理逻辑”。
三、特征工程数学规则
派生特征构建：
基于数据特点创建有业务意义的新特征，按 “行业 - 场景 - 目标 - 特征 - 公式” 设计，示例：金融信贷风控场景，目标为 “评估用户还款能力”，构建特征 “月还款压力 = 月还款金额 / 月收入”，公式为月还款压力 =（每月需还款总额）÷（每月薪资收入），该特征可直接反映用户收入与还款的匹配度。
特征交互：探索特征间的潜在关系，说明交互逻辑与业务意义，示例：零售场景 “商品类别（分类型：食品 = 1，日用品 = 2）” 与 “促销折扣（数值型：0.8,0.9）” 交互，构建交叉特征 “类别_折扣”，取值为 “1×0.8=0.8（食品折扣），2×0.9=1.8（日用品折扣）”，可区分不同类别商品的折扣力度。
聚合特征：如适用，提供分组聚合特征创建方法，按 “分组字段 - 聚合维度 - 特征 - 公式” 说明，示例：制造场景按 “生产线 ID” 分组，聚合维度为 “近 7 天”，构建特征 “日均产量”，公式为日均产量 =（近 7 天该生产线总产量）÷7，可反映生产线短期生产效率。
特征筛选机制：
相关性分析：使用统计方法评估特征间相关性，采用皮尔逊相关系数，公式为 r=Σ[(xi-μx)(yi-μy)]/√[Σ(xi-μx)²Σ(yi-μy)²]，xi、yi 为两个特征的样本值，μx、μy 为对应特征的均值，示例：金融场景 “月收入” 与 “月可支配收入” 相关系数 r=0.92（|r|>0.8），判定高度相关，因 “月收入” 数据更完整，剔除 “月可支配收入” 避免冗余。
重要性评估：提供特征重要性评估方法，结合行业适配模型选择，示例：医疗场景需模型可解释性，选用逻辑回归评估特征重要性，“诊断结果” 特征系数绝对值 0.8（高于其他特征 0.3-0.5），判定为重要特征必须保留，因该特征关联疾病判断核心逻辑。
降维策略：如需要，提供适当的降维方法，示例：特征数 > 30 时用 PCA 降维，步骤为计算特征协方差矩阵、求解特征值、选取特征值≥1 的主成分、验证累计方差贡献率≥85%，制造场景特征数 35，保留特征值 5.2、3.8 的主成分（累计贡献率 85.6%），最终降维后特征数为 2。
四、模型应用与行业适配
建模方向建议：
基于数据特点推荐合适的建模类型（分类、回归、聚类等），结合行业常见目标说明，示例：医疗场景目标 “判断患者是否患糖尿病（是 / 否）”，为二分类问题，推荐逻辑回归（可解释性强）或 SVM（小样本精度高）；目标 “预测未来 3 个月血糖值”，为回归问题，推荐线性回归（线性关系明显时）或 LightGBM（含非线性特征时）。
提供模型选择的基本原则，补充行业约束条件，示例：金融风控场景需满足 “可解释性 + 合规性”，排除深度学习等黑箱模型，优先选择 XGBoost（集成模型精度高），搭配 SHAP 值解释输出；医疗场景需 “低误判率 + 可解释性”，优先选择逻辑回归或随机森林。
跨领域通用性：
提供数据处理方法在不同行业场景的通用化建议，按 “规则类型 - 通用范围 - 调整要点” 说明，示例：缺失值填充规则中，数值型用中位数、分类型用众数可跨行业通用；调整要点为医疗生理指标（如心率）缺失率 > 30% 时，需用线性插值法，因生理指标需保持医学合理性。
强调数据预处理方法的普适性，总结 “通用规则 + 行业专属调整项”，示例：数据清洗的重复值删除、极端值替换为通用标准，仅需按行业补充专属约束（如制造 “生产数量≤设备产能”、零售 “销售价格≥成本价”），无需重构规则体系。
--- 输出规范 ---
文本格式：纯技术文档表述，禁用任何特殊符号（包括箭头 / 项目符号等）
代码转换规则说明：对提及的技术方法进行文字化技术描述
专业深度：特征工程需带数学表达式推导过程
拒绝简化：扩展 "无需处理" 为完整技术说明
字数：800-1000 字符四层完整覆盖
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
    
