import string

from flask import Flask, request, g as global_data

# coordinator for two-phase commit
app = Flask(__name__)
app.config.from_pyfile("config.py")


@app.route('/')
def hello_world():
    return {"response": 'Hello World!'}


@app.route('/observe')
def insert_observation():
    global_data.a = ''
    query = request.args.get('query')
    return {'response': query}


if __name__ == '__main__':
    app.run()
