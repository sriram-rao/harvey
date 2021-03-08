from flask import Flask

# coordinator for two-phase commit
app = Flask(__name__)


@app.route('/')
def hello_world():
    return {"response": 'Hello World!'}


if __name__ == '__main__':
    app.run()
