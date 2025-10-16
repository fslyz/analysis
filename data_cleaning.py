import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import os
import sqlite3
import tempfile
import asyncio
import re
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain.agents import AgentExecutor
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from openpyxl import Workbook
from openpyxl.styles import Border, Side, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
import win32com.client as win32
import pythoncom
from fpdf import FPDF

# API配置
API_KEY = "sk-062b1fc6f6a9425eae59020faa7b5f6e"
API_BASE = "https://api.deepseek.com/v1"
# 模型配置
MODEL_NAME = "deepseek-chat"
MODEL_TEMPERATURE = 0

# 输出路径（可根据需要修改）
OUTPUT_PATH = "F:\\analysis\\data"


class DataProcessor:
    def __init__(self):
        # 初始化大模型
        self.llm = ChatOpenAI(
            openai_api_key=API_KEY,
            openai_api_base=API_BASE,
            model_name=MODEL_NAME,
            temperature=MODEL_TEMPERATURE
        )
        # 确保输出目录存在
        self._ensure_output_directory()

    def _ensure_output_directory(self):
        """确保输出目录存在"""
        try:
            os.makedirs(OUTPUT_PATH, exist_ok=True)
            print(f"输出目录已确认: {OUTPUT_PATH}")
        except Exception as e:
            print(f"创建输出目录失败: {str(e)}")
            raise

    def create_temp_sql_db(self, file_path):
        """创建临时数据库并导入数据"""
        try:
            # 读取数据文件
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                return None, "错误：仅支持CSV和Excel格式"

            # 创建临时数据库
            temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
            temp_db.close()

            # 导入数据
            conn = sqlite3.connect(temp_db.name)
            table_name = re.sub(r'[^a-zA-Z0-9_]', '_', 
                               os.path.splitext(os.path.basename(file_path))[0])
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()

            db = SQLDatabase.from_uri(f"sqlite:///{temp_db.name}")
            return db, temp_db.name, df, table_name

        except Exception as e:
            return None, f"创建数据库失败: {str(e)}"

    def apply_pandas_cleaning(self, df, cleaning_operations):
        """执行数据清洗"""
        try:
            for operation in cleaning_operations:
                op_type = operation.get('type')

                if op_type == 'remove_duplicates':
                    subset = operation.get('subset')
                    keep = operation.get('keep', 'first')
                    df = df.drop_duplicates(subset=subset, keep=keep)

                elif op_type == 'fill_missing':
                    column = operation.get('column')
                    method = operation.get('method', 'mean')
                    value = operation.get('value')

                    if method == 'mean':
                        df[column].fillna(df[column].mean(), inplace=True)
                    elif method == 'median':
                        df[column].fillna(df[column].median(), inplace=True)
                    elif method == 'mode':
                        df[column].fillna(df[column].mode()[0], inplace=True)
                    elif method == 'forward_fill':
                        df[column].fillna(method='ffill', inplace=True)
                    elif method == 'backward_fill':
                        df[column].fillna(method='bfill', inplace=True)
                    elif value is not None:
                        df[column].fillna(value, inplace=True)

                elif op_type == 'remove_outliers':
                    column = operation.get('column')
                    Q1 = df[column].quantile(0.25)
                    Q3 = df[column].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

                elif op_type == 'normalize_text':
                    column = operation.get('column')
                    for op in operation.get('operations', []):
                        if op == 'lower':
                            df[column] = df[column].str.lower()
                        elif op == 'upper':
                            df[column] = df[column].str.upper()
                        elif op == 'strip':
                            df[column] = df[column].str.strip()
                        elif op == 'remove_punctuation':
                            import string
                            df[column] = df[column].str.translate(
                                str.maketrans('', '', string.punctuation))

            return df
        except Exception as e:
            print(f"清洗出错: {str(e)}")
            return df

    def _create_temp_excel(self, df):
        """创建临时Excel（优化格式）"""
        try:
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_excel:
                temp_path = temp_excel.name

            wb = Workbook()
            ws = wb.active
            ws.title = "清洗后数据"

            # 写入数据并设置格式
            for row_idx, row_data in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
                for col_idx, cell_value in enumerate(row_data, 1):
                    value = str(cell_value) if pd.notna(cell_value) else ""
                    cell = ws.cell(row=row_idx, column=col_idx, value=value)
                    # 关键：文字自动换行
                    cell.alignment = Alignment(
                        wrap_text=True,
                        vertical='center',
                        horizontal='left'
                    )

            # 自动调整列宽
            self._auto_adjust_column_width(ws, df)

            # 设置行高
            ws.row_dimensions[1].height = 28
            for row in range(2, ws.max_row + 1):
                ws.row_dimensions[row].height = 35

            # 添加边框
            self._add_cell_borders(ws)

            wb.save(temp_path)
            return temp_path
        except Exception as e:
            print(f"创建Excel失败: {str(e)}")
            return None

    def _auto_adjust_column_width(self, worksheet, df):
        """自动调整列宽"""
        column_max_length = {}
        # 计算表头长度
        for col_idx, col_name in enumerate(df.columns, 1):
            column_max_length[col_idx] = len(str(col_name)) + 3
        # 计算数据长度
        for col_idx, col_name in enumerate(df.columns, 1):
            for value in df[col_name]:
                value_length = len(str(value)) + 2
                if value_length > column_max_length[col_idx]:
                    column_max_length[col_idx] = value_length

        # 设置列宽（限制最大宽度）
        max_width = 22
        for col_idx, max_length in column_max_length.items():
            col_width = min(max_length * 0.9, max_width)
            worksheet.column_dimensions[get_column_letter(col_idx)].width = col_width

    def _add_cell_borders(self, worksheet):
        """添加单元格边框"""
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        for row in worksheet.iter_rows(
            min_row=1, max_row=worksheet.max_row,
            min_col=1, max_col=worksheet.max_column
        ):
            for cell in row:
                cell.border = thin_border

    def get_available_font(self):
        """获取可用中文字体"""
        font_candidates = [
            'C:/Windows/Fonts/simhei.ttf',
            'C:/Windows/Fonts/msyh.ttf',
            'C:/Windows/Fonts/simsun.ttc',
            '/Library/Fonts/PingFang.ttc',
            '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',
        ]
        for font_path in font_candidates:
            if os.path.exists(font_path):
                return font_path
        return None

    def _excel_to_pdf(self, excel_path, pdf_path, max_rows=300):
        """Excel转PDF（纵向分页）"""
        if not os.path.exists(excel_path):
            print(f"Excel不存在: {excel_path}")
            return False

        try:
            # Windows系统用Excel COM（最佳效果）
            if os.name == 'nt':
                return self._excel_to_pdf_via_com(excel_path, pdf_path, max_rows)
            # 其他系统用FPDF模拟
            return self._excel_to_pdf_via_fpdf(excel_path, pdf_path, max_rows)
        except Exception as e:
            print(f"PDF转换失败: {str(e)}")
            return False

    def _excel_to_pdf_via_com(self, excel_path, pdf_path, max_rows):
        """Windows专用：Excel COM转换PDF"""
        pythoncom.CoInitialize()
        try:
            excel = win32.gencache.EnsureDispatch('Excel.Application')
            excel.Visible = False
            workbook = excel.Workbooks.Open(
                Filename=os.path.abspath(excel_path),
                ReadOnly=True
            )
            worksheet = workbook.ActiveSheet

            # 限制最大行数
            total_rows = worksheet.UsedRange.Rows.Count
            if total_rows > max_rows:
                for row in range(max_rows + 1, total_rows + 1):
                    worksheet.Rows(row).Hidden = True

            # 关键设置：纵向分页
            worksheet.PageSetup.Orientation = 1  # 纵向
            worksheet.PageSetup.FitToPagesWide = 1  # 宽度适配1页
            worksheet.PageSetup.PrintTitleRows = "$1:$1"  # 每页显示表头

            # 导出PDF
            worksheet.ExportAsFixedFormat(
                Type=0,
                Filename=os.path.abspath(pdf_path)
            )

            workbook.Close(False)
            excel.Quit()
            print(f"PDF生成成功: {pdf_path}")
            return True
        except Exception as e:
            print(f"COM转换出错: {str(e)}")
            return False
        finally:
            pythoncom.CoUninitialize()

    def _excel_to_pdf_via_fpdf(self, excel_path, pdf_path, max_rows):
        """非Windows系统：FPDF转换PDF"""
        try:
            df = pd.read_excel(excel_path)
            if len(df) > max_rows:
                df = df.head(max_rows)

            # A4纵向
            pdf = FPDF(orientation='P', unit='mm', format='A4')
            pdf.add_page()

            # 设置字体
            font_path = self.get_available_font()
            font_name = 'Arial'
            if font_path:
                try:
                    pdf.add_font('Chinese', '', font_path, uni=True)
                    pdf.set_font('Chinese', size=9)
                    font_name = 'Chinese'
                except:
                    pdf.set_font('Arial', size=9)
            else:
                pdf.set_font('Arial', size=9)

            # 计算单页最大列数
            page_width = pdf.w - 20
            col_width = 18
            max_cols_per_page = int(page_width // col_width)

            # 拆分列组（实现右侧分页）
            columns = list(df.columns)
            col_groups = [
                columns[i:i + max_cols_per_page] 
                for i in range(0, len(columns), max_cols_per_page)
            ]

            # 生成每页内容
            for group_idx, col_group in enumerate(col_groups, 1):
                if group_idx > 1:
                    pdf.add_page()
                    pdf.set_font(font_name, size=9)

                # 表头
                pdf.set_fill_color(240, 240, 240)
                for col_name in col_group:
                    pdf.multi_cell(col_width, 8, str(col_name), border=1, 
                                  align='C', fill=True, ln=3)
                pdf.ln()

                # 数据行
                pdf.set_fill_color(255, 255, 255)
                for _, row in df.iterrows():
                    if pdf.get_y() > 270:
                        pdf.add_page()
                        pdf.set_font(font_name, size=9)
                        # 重新打印表头
                        pdf.set_fill_color(240, 240, 240)
                        for col_name in col_group:
                            pdf.multi_cell(col_width, 8, str(col_name), border=1, 
                                          align='C', fill=True, ln=3)
                        pdf.ln()
                        pdf.set_fill_color(255, 255, 255)

                    # 写入数据
                    for col_name in col_group:
                        value = str(row[col_name]) if pd.notna(row[col_name]) else ""
                        pdf.multi_cell(col_width, 8, value, border=1, 
                                      align='L', fill=False, ln=3)
                    pdf.ln()

            pdf.output(pdf_path)
            print(f"PDF生成成功: {pdf_path}")
            return True
        except Exception as e:
            print(f"FPDF转换出错: {str(e)}")
            return False

    async def _execute_query(self, agent_executor, prompt, query_name):
        """执行查询"""
        print(f"===== 执行查询：{query_name} =====")
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, lambda: agent_executor.invoke(prompt)
            )
            return query_name, result['output']
        except Exception as e:
            print(f"查询出错: {str(e)}")
            return query_name, f"查询失败: {str(e)}"

    async def _analyze_and_generate_sql(self, query_results):
        """分析数据并生成清洗SQL"""
        print("===== 分析数据质量 =====")
        
        analysis_prompt = f"""
        基于以下查询结果，分析数据质量并生成清洗SQL：

        基本信息：{query_results.get('基本信息', '无')}
        缺失值：{query_results.get('缺失值分析', '无')}
        重复数据：{query_results.get('重复数据分析', '无')}
        数值列：{query_results.get('数值列分析', '无')}
        文本列：{query_results.get('文本列分析', '无')}

        请提供：
        1. 数据集概述
        2. 质量问题
        3. 清洗建议
        4. 清洗SQL（前缀"EXECUTE SQL:"）
        """

        messages = [
            SystemMessage(content="专业数据分析师，擅长SQL清洗"),
            HumanMessage(content=analysis_prompt)
        ]

        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: self.llm(messages)
        )
        return response.content

    def _extract_sql_statements(self, analysis_text):
        """提取SQL语句"""
        sql_pattern = r'EXECUTE SQL:\s*(.*?)(?=\n(?:EXECUTE SQL:|$))'
        sql_matches = re.findall(sql_pattern, analysis_text, re.DOTALL)
        
        return [
            '\n'.join(line.strip() for line in match.split('\n') if line.strip())
            for match in sql_matches if match.strip()
        ]

    async def _execute_sql_modifications(self, agent_executor, sql_statements, table_name):
        """执行SQL清洗"""
        if not sql_statements:
            print("无清洗语句")
            return

        print(f"===== 执行{len(sql_statements)}条清洗SQL =====")
        for idx, sql in enumerate(sql_statements, 1):
            print(f"\n第{idx}条SQL: {sql}")
            try:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: agent_executor.invoke(f"执行SQL: {sql}")
                )
                print(f"结果: {result['output']}")
            except Exception as e:
                print(f"执行出错: {str(e)}")

    def process_dataset(self, file_path):
        """主流程"""
        try:
            # 创建临时数据库
            db_result = self.create_temp_sql_db(file_path)
            if not isinstance(db_result, tuple) or len(db_result) < 4:
                return db_result[1] if isinstance(db_result, tuple) else str(db_result)

            db, temp_db_path, _, table_name = db_result

            # 创建SQL代理
            toolkit = SQLDatabaseToolkit(db=db, llm=self.llm)
            agent_executor = create_sql_agent(
                llm=self.llm,
                toolkit=toolkit,
                verbose=True,
                agent_type="openai-tools",
            )

            # 执行查询
            queries = [
                ("基本信息", f"查询'{table_name}'的行数、列数、列名和类型"),
                ("缺失值分析", f"统计'{table_name}'每列缺失值数量和比例"),
                ("重复数据分析", f"检查'{table_name}'重复数据情况"),
                ("数值列分析", f"分析'{table_name}'数值列统计信息"),
                ("文本列分析", f"分析'{table_name}'文本列格式")
            ]

            async def run_queries():
                return await asyncio.gather(*[
                    self._execute_query(agent_executor, prompt, name)
                    for name, prompt in queries
                ])

            query_results = dict(asyncio.run(run_queries()))

            # 分析并生成清洗SQL
            analysis_result = asyncio.run(self._analyze_and_generate_sql(query_results))
            sql_statements = self._extract_sql_statements(analysis_result)

            # 执行清洗并验证
            async def process_cleaning():
                if sql_statements:
                    await self._execute_sql_modifications(agent_executor, sql_statements, table_name)
                    verification = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: agent_executor.invoke(f"验证'{table_name}'清洗效果")
                    )
                    return verification['output']
                return "未执行清洗"

            verification_result = asyncio.run(process_cleaning())

            # 生成PDF
            conn = sqlite3.connect(temp_db_path)
            final_df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            conn.close()

            temp_excel = self._create_temp_excel(final_df)
            if not temp_excel:
                return "生成PDF失败：临时Excel创建失败"

            base_name = os.path.splitext(os.path.basename(file_path))[0]
            pdf_path = os.path.join(OUTPUT_PATH, f"{base_name}_样本示例.pdf")

            pdf_success = self._excel_to_pdf(temp_excel, pdf_path)

            # 清理临时文件
            try:
                os.remove(temp_excel)
                os.remove(temp_db_path)
            except Exception as e:
                print(f"清理临时文件出错: {str(e)}")

            if pdf_success:
                return f"""
===== 处理完成 =====

1. 查询结果:
{chr(10).join(f'{k}:\n{v}\n' for k, v in query_results.items())}

2. 分析与清洗建议:
{analysis_result}

3. 清洗验证:
{verification_result}

4. 输出文件:
{pdf_path}
"""
        except Exception as e:
            return f"处理出错: {str(e)}"

if __name__ == "__main__":
    processor = DataProcessor()
    file_path = input("请输入数据集文件路径: ")
    
    if os.path.exists(file_path):
        result = processor.process_dataset(file_path)
        print(result)
    else:
        print("文件不存在，请检查路径")
