from flask import Flask, request
from dotenv import load_dotenv
import os


load_dotenv()

app = Flask(__name__)


@app.route('/', methods=['POST', 'GET'])
def dummy_server():
    if request.method == 'POST':
        data = request.data.decode('utf-8')
        print("Received request:", data)
        return "Request received by dummy server\n"
    else:
        return "Welcome to the dummy server 1\n"


if __name__ == '__main__':
    app.run(host=os.getenv("PRIVATE_IP"), port=5000)
