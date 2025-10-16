import asyncio
import os
from data_reader import read_file_data
from data_algorithm import analyze_dataset as algorithm_analyze, save_to_pdf as algorithm_save
from data_collection import analyze_dataset as collection_analyze, save_to_text as collection_save
from data_dictionary import analyze_dataset as dictionary_analyze, save_to_excel as dictionary_save
from data_application import analyze_dataset as application_analyze, save_to_text as application_save
# 新增：导入数据清洗模块
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
    # 保存数据应用
    if results[3]:
        application_path = application_save(results[3], dataset_name)
        print(f"数据应用场景已保存至: {application_path}")
    else:
        print("数据应用处理结果为空，未能保存")
    
    # 新增：打印数据清洗结果（results[4]对应数据清洗任务的返回值）
    print("\n数据清洗处理结果：")
    print(results[4])
    
    print("所有结果保存完成！")

if __name__ == "__main__":
    # 运行主程序
    asyncio.run(main())
