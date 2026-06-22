# app.py
from flask import Flask
app = Flask(__name__)

# 定义一个测试接口
@app.route('/text')
def hello():
    return {"text":"isRunning"}

if __name__ == '__main__':
    # 关键：host=0.0.0.0 允许外部访问，port=5000 暴露端口
    app.run(host='0.0.0.0', port=5000)
