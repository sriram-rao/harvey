from flask import Flask, request
from pgRepo import PostgresRepo
import string

# cohort layer for two-phase commit

app = Flask(__name__)
app.config.from_pyfile("config.py")
repo = PostgresRepo(app.config.get('POSTGRES_PORT'))


@app.route('/')
def hello_world():
    repo.log("test", "hello")
    return {'response': 'Hello World!'}


@app.route('/observe')
def insert_observation():
    query = request.args.get('query')
    return {'response': query}


@app.route('/prepare/<transaction>')
def prepare(transaction: string):
    repo.log(transaction, "prepare")
    if not repo.prepare_commit(transaction):
        return {'result': 'no'}
    return {'result': 'yes'}


@app.route('/commit/<transaction>')
def commit(transaction: string):
    repo.log(transaction, "commit")
    repo.commit_prepared(transaction)
    return {'result': 'success'}


@app.route('/abort/<transaction>')
def abort(transaction: string):
    repo.log(transaction, "abort")
    repo.abort_prepared(transaction)
    return {'result': 'success'}


if __name__ == '__main__':
    # add recovery tasks
    app.run()
