from flask import Flask, request
import requests

from pgRepo import PostgresRepo
import string

# cohort layer for two-phase commit


def recover(port: int):
    # if prepared, get status from coordinator
    prepared_count = repo.get_prepared_count()
    if prepared_count > 0:
        # get my status
        name, status = repo.get_last_status()
        url = f"http://localhost:{coordinator}/status/{name}/{port}"
        response = requests.get(url)
        recovery_status = response.json()['status']
        if recovery_status == 'complete':
            return
        if recovery_status == 'commit':
            repo.recover_commit_prepared(name)
            action = 'commit'
        elif recovery_status == 'abort':
            repo.recover_abort_prepared(name)
            action = 'abort'
        url = f"http://localhost:{coordinator}/register?action={action}&name={name}&cohort={port}"
        requests.get(url)


app = Flask(__name__)
app.config.from_pyfile("config.py")
repo = PostgresRepo(app.config.get('POSTGRES_PORT'))
coordinator = app.config.get('COORDINATOR')
recover(app.config.get('MY_PORT'))


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
        try:
            repo.abort_prepared()
            repo.log(params['transaction'], "abort")
        except Exception:
            pass
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
    app.run()
