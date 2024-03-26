from flask import Flask, request
from dotenv import load_dotenv
import os
import sys


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
    if len(sys.argv) != 3:
        print(
            "Usage: python server_simulator.py <server_name> <port>")
        sys.exit(1)

    host = os.getenv("PRIVATE_IP")
    port = int(sys.argv[2])

    print(f"Server {sys.argv[1]} is initiated and running on {host}:{port}")

    app.run(host = host, port=port)
