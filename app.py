# app.py
import os
import zipfile
import tempfile
import time
from flask import Flask, request, send_file, jsonify, render_template
from flask_cors import CORS
import main  # 导入你的主处理模块
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
app = Flask(__name__)
CORS(app)  # 解决跨域问题
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 限制50MB

@app.route('/')
def index():
    return render_template('report-generator.html')

@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    # 检查是否有文件上传
    if 'datasets[]' not in request.files and 'dataset' not in request.files:
        return jsonify({"error": "未上传文件"}), 400

    # 确定是单个文件还是多个文件
    if 'datasets[]' in request.files:
        files = request.files.getlist('datasets[]')
    else:
        files = [request.files['dataset']]

    # 过滤掉空文件
    files = [f for f in files if f.filename != '']
    if not files:
        return jsonify({"error": "未选择文件"}), 400

    # 清空data文件夹中的文件
    data_dir = os.path.join(os.getcwd(), "data")
    if os.path.exists(data_dir):
        for filename in os.listdir(data_dir):
            file_path = os.path.join(data_dir, filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    import shutil
                    shutil.rmtree(file_path)
            except Exception as e:
                app.logger.warning(f"无法删除文件 {file_path}: {str(e)}")

    temp_paths = []
    zip_path = None

    try:
        # 保存所有上传的临时文件
        for file in files:
            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as temp_file:
                file.save(temp_file)
                temp_paths.append(temp_file.name)

        # 创建临时ZIP文件
        zip_fd, zip_path = tempfile.mkstemp(suffix='.zip')
        os.close(zip_fd)  # 关闭文件描述符，但保留路径

        try:
            # 使用线程池并行处理数据集，最多支持10个并发线程
            with ThreadPoolExecutor(max_workers=min(len(files), 10)) as executor:
                # 提交所有任务
                future_to_file = {
                    executor.submit(main.process_dataset, temp_paths[i], os.path.splitext(files[i].filename)[0]): files[i]
                    for i in range(len(files))
                }
                
                # 存储结果
                results = {}
                
                # 收集所有任务结果
                for future in as_completed(future_to_file):
                    file = future_to_file[future]
                    try:
                        report_paths = future.result()
                        results[file.filename] = report_paths
                        app.logger.info(f"成功处理文件: {file.filename}")
                    except Exception as e:
                        app.logger.error(f"处理文件 {file.filename} 时出错: {str(e)}")
                        results[file.filename] = None
            
            # 创建ZIP文件并添加所有报告
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file in files:
                    report_paths = results.get(file.filename)
                    
                    # 检查报告路径是否有效
                    if not report_paths:
                        app.logger.warning(f"未能为文件 {file.filename} 生成任何报告文件")
                        continue
                    
                    # 获取原始文件名（不含扩展名）
                    original_name = os.path.splitext(file.filename)[0]
                    
                    # 创建文件夹名称，使用原始文件名
                    folder_name = f"{original_name}_数据报告"
                    
                    for report_path in report_paths:
                        # 确保文件存在
                        if os.path.exists(report_path):
                            # 将文件放入文件夹中
                            zipf.write(report_path, f"{folder_name}/{os.path.basename(report_path)}")
                        else:
                            app.logger.warning(f"报告文件不存在: {report_path}")

            # 返回ZIP文件
            if len(files) == 1:
                download_name = f"{os.path.splitext(files[0].filename)[0]}_数据报告.zip"
            else:
                download_name = f"批量数据报告_{len(files)}个数据集_{int(time.time())}.zip"

            return send_file(
                zip_path,
                mimetype='application/zip',
                as_attachment=True,
                download_name=download_name
            )
        except Exception as e:
            # 如果出错，尝试删除已创建的ZIP文件
            if zip_path and os.path.exists(zip_path):
                try:
                    os.unlink(zip_path)
                except Exception:
                    pass
            raise

    except Exception as e:
        import traceback
        app.logger.error(f"处理上传文件时出错: {str(e)}")
        app.logger.error(f"错误详情: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500
    finally:
        # 清理临时文件
        for temp_path in temp_paths:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception as e:
                    app.logger.warning(f"无法删除临时文件 {temp_path}: {str(e)}")

        # 注意：ZIP文件由Flask在发送后自动清理，不需要手动删除

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
