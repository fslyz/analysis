# app.py
import os
import zipfile
import tempfile
from flask import Flask, request, send_file, jsonify, render_template
from flask_cors import CORS
import main  # 导入你的主处理模块

app = Flask(__name__)
CORS(app)  # 解决跨域问题
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 限制50MB

@app.route('/')
def index():
    return render_template('report-generator.html')

@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    if 'dataset' not in request.files:
        return jsonify({"error": "未上传文件"}), 400
    
    file = request.files['dataset']
    if file.filename == '':
        return jsonify({"error": "未选择文件"}), 400

    try:
        # 保存上传的临时文件
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as temp_file:
            file.save(temp_file)
            temp_path = temp_file.name

        # 获取原始文件名（不含扩展名）
        original_name = os.path.splitext(file.filename)[0]
        
        # 调用main函数生成五个报告（假设返回报告文件路径列表）
        report_paths = main.process_dataset(temp_path, original_name)
        
        # 创建临时ZIP文件
        zip_path = tempfile.mktemp(suffix='.zip')
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for report_path in report_paths:
                    zipf.write(report_path, os.path.basename(report_path))
            
            # 返回ZIP文件
            return send_file(
                zip_path,
                mimetype='application/zip',
                as_attachment=True,
                download_name=f"{os.path.splitext(file.filename)[0]}_数据报告.zip"
            )
        except Exception as e:
            # 如果出错，尝试删除已创建的ZIP文件
            if os.path.exists(zip_path):
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
        try:
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.unlink(temp_path)
        except Exception as e:
            app.logger.warning(f"无法删除临时文件 {temp_path}: {str(e)}")
            
        # ZIP文件不再尝试删除，让系统自动处理临时文件

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)