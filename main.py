import asyncio
import os
from data_reader import read_file_data
from data_algorithm import analyze_dataset as algorithm_analyze, save_to_pdf as algorithm_save
from data_collection import analyze_dataset as collection_analyze, save_to_text as collection_save
from data_dictionary import analyze_dataset as dictionary_analyze, save_to_excel as dictionary_save
from data_application import analyze_dataset as application_analyze, save_to_text as application_save
from data_cleaning import DataProcessor

async def main():
    """
    主程序：读取数据并异步处理
    """
    print("开始数据处理流程...")

    # 1. 使用data_reader读取数据集
    print("正在读取数据集...")
    file_path = input("请输入Excel/CSV文件完整路径：").strip()
    df, dataset_name = read_file_data(file_path)
    print(f"数据读取完成，共{len(df)}条记录")

    # 2. 并行运行五个处理模块（包含数据清洗）
    print("开始并行处理数据...")
    # 初始化数据清洗处理器
    data_processor = DataProcessor()
    tasks = [
        asyncio.to_thread(algorithm_analyze, dataset_name, dataframe=df),   # 算法处理
        asyncio.to_thread(collection_analyze, dataset_name, dataframe=df),  # 数据收集处理
        asyncio.to_thread(dictionary_analyze, dataset_name, dataframe=df),   # 数据字典处理
        asyncio.to_thread(application_analyze, dataset_name, dataframe=df),  # 数据应用处理
        asyncio.to_thread(data_processor.process_dataset, file_path)# 数据清洗处理
    ]

    # 等待所有任务完成
    results = await asyncio.gather(*tasks)
    print("所有数据处理任务已完成！")

    # 3. 保存原有四个模块的结果
    print("正在保存结果...")
    # 保存算法规则表
    if results[0]:
        algorithm_path = algorithm_save(results[0], dataset_name)
        print(f"算法规则已保存至: {algorithm_path}")
    else:
        print("算法处理结果为空，未能保存")
    # 保存数据收集情况
    if results[1]:
        collection_path = collection_save(results[1], dataset_name)
        print(f"数据收集情况已保存至: {collection_path}")
    else:
        print("数据收集处理结果为空，未能保存")
    # 保存数据字典
    if results[2]:
        dictionary_path = dictionary_save(results[2], dataset_name)
        print(f"数据字典已保存至: {dictionary_path}")
    else:
        print("数据字典处理结果为空，未能保存")
    # 保存数据应用场景
    if results[3]:
        application_path = application_save(results[3], dataset_name)
        print(f"数据应用场景已保存至: {application_path}")
    else:
        print("数据应用处理结果为空，未能保存")
    
    # 新增：打印数据清洗结果（results[4]对应数据清洗任务的返回值）
    print("\n数据清洗处理结果：")
    print(results[4])
    
    print("所有结果保存完成！")

def process_dataset(file_path, original_name=None):
    """
    处理上传的数据集，生成五个报告
    返回报告文件路径列表
    
    参数:
        file_path: 数据集文件路径
        original_name: 原始文件名（不含扩展名），用于命名报告文件
    """
    # 1. 读取数据集
    df, dataset_name = read_file_data(file_path)
    
    # 如果提供了原始文件名，则使用它来替代dataset_name
    if original_name:
        dataset_name = original_name
    
    # 2. 初始化数据清洗处理器
    data_processor = DataProcessor()
    
    # 3. 处理算法规则表
    algorithm_result = algorithm_analyze(dataset_name, dataframe=df)
    algorithm_path = None
    if algorithm_result:
        algorithm_path = algorithm_save(algorithm_result, dataset_name)
    
    # 4. 处理数据收集情况
    collection_result = collection_analyze(dataset_name, dataframe=df)
    collection_path = None
    if collection_result:
        collection_path = collection_save(collection_result, dataset_name)
    
    # 5. 处理数据字典
    dictionary_result = dictionary_analyze(dataset_name, dataframe=df)
    dictionary_path = None
    if dictionary_result:
        dictionary_path = dictionary_save(dictionary_result, dataset_name)
    
    # 6. 处理数据应用场景
    application_result = application_analyze(dataset_name, dataframe=df)
    application_path = None
    if application_result:
        application_path = application_save(application_result, dataset_name)
    
    # 7. 数据清洗处理（但不生成报告文件）
    # 只执行数据清洗，但不保存清洗报告
    data_processor.process_dataset(file_path, original_name)
    
    # 查找样本示例文件（现在应该已经使用正确的名称）
    data_dir = "F:\\analysis\\data"  # 确认data目录
    sample_path = None
    expected_sample_path = os.path.join(data_dir, f"{dataset_name}_样本示例.pdf")
    
    # 检查样本示例文件是否存在
    if os.path.exists(expected_sample_path):
        sample_path = expected_sample_path
        print(f"找到样本示例文件: {sample_path}")
    
    # 8. 返回所有生成的报告路径
    report_paths = []
    if algorithm_path:
        report_paths.append(algorithm_path)
    if collection_path:
        report_paths.append(collection_path)
    if dictionary_path:
        report_paths.append(dictionary_path)
    if application_path:
        report_paths.append(application_path)
    if sample_path:
        report_paths.append(sample_path)
    
    return report_paths

if __name__ == "__main__":
    # 运行主程序
    asyncio.run(main())
