from flask import Flask, request
from pgRepo import PostgresRepo
import string

# cohort layer for two-phase commit

app = Flask(__name__)
app.config.from_pyfile("config.py")
repo = PostgresRepo(app.config.get('POSTGRES_PORT'))


@app.route('/')
def hello_world():
    return {'response': 'Welcome to the Harvey Dent commit protocol!'}


@app.route('/observe', methods=['GET', 'POST'])
def insert_observation():
    params = request.get_json()
    sensor_type = params['type']
    data = params['data']
    try:
        repo.log(params['transaction'], "prepare")
        repo.observe(sensor_type, params['transaction'], data)
        return {'result': 'yes'}
    except Exception:
        return {'result': 'no'}


@app.route('/commit/<transaction>')
def commit(transaction: string):
    repo.commit_prepared()
    repo.log(transaction, "commit")
    return {'result': 'success'}


@app.route('/abort/<transaction>')
def abort(transaction: string):
    repo.abort_prepared()
    repo.log(transaction, "abort")
    return {'result': 'success'}


if __name__ == '__main__':
    # add recovery tasks
    # transaction_id = repo.get_transaction_id(transaction)
    # repo.recover_commit_prepared(transaction)
    app.run()
