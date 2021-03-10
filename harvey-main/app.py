import datetime
import string

import requests
from flask import Flask, request, g as global_data

from pgRepo import PostgresRepo

# coordinator for two-phase commit

app = Flask(__name__)
app.config.from_pyfile("config.py")
global_data.transaction_batch = app.config.get('BATCH_SIZE')
global_data.active_count = 0
global_data.active_transactions = dict()
global_data.transaction_name = ''

repo = PostgresRepo(app.config.get('POSTGRES_PORT'))
cohort_ports = list(app.config.get('COHORT_PORTS'))


@app.route('/')
def hello_world():
    return {"response": 'Hello World!'}


@app.route('/observe', methods=['GET', 'POST'])
def insert_observation():
    params = request.get_json()
    data = params['data']
    cohort = cohort_ports[get_hash(data['sensor_id'], data['timeStamp']) % len(cohort_ports)]
    to_abort = False

    if global_data.active_count == 0:
        global_data.transaction_name = 'trx' + str(cohort)

    if cohort not in global_data.active_transactions:
        global_data.active_transactions[cohort] = []
    global_data.active_transactions[cohort].append(data)
    global_data.active_count += 1

    if global_data.active_count < 10:
        return {'result': 'success'}

    if global_data.active_count > global_data.transaction_batch:
        if not prepare():
            to_abort = True
            abort()
        commit()
        clear_global_state()

    return {'result': f"{'abort' if to_abort else 'success'}"}


@app.route('/status/<transaction>')
def get_status(transaction: string):
    # get status from protocol table and return
    pass


def get_hash(sensor: string, time: datetime) -> int:
    hash_string = f"{sensor}_{time}"
    return hash_string.__hash__()


def begin_transaction(cohort: int, transaction: string):
    url = f"http://localhost:{cohort}/begin/{transaction}"
    response = requests.get(url)
    return response.json()['result'] == 'success'


def post_data(port: int, data: dict) -> bool:
    url = f"http://localhost:{port}/observe"
    response = requests.post(url, data=data)
    return response.json()['result'] == 'success'


def prepare() -> bool:
    for cohort in cohort_ports:
        data = global_data.active_transactions[cohort]
        url = f"http://localhost:{cohort}/observe"
        response = requests.post(url, data=data)
        if response.json()['result'] == 'no':
            return False
        repo.log(global_data.transaction_name, cohort, 'prepared')
    return True


def abort() -> bool:
    transaction = global_data.transaction_name
    responses = {}
    for cohort in cohort_ports:
        repo.log(transaction, cohort, 'abort')
        # abort cohorts
        url = f"http://localhost:{cohort}/abort/{transaction}"
        responses[cohort] = requests.get(url)
        repo.remove_log(transaction, cohort, 'abort')
    return True


def commit() -> bool:
    transaction = global_data.transaction_name
    responses = {}
    for cohort in cohort_ports:
        repo.log(transaction, cohort, 'commit')
        # abort cohorts
        url = f"http://localhost:{cohort}/commit/{transaction}"
        responses[cohort] = requests.get(url)
        repo.remove_log(transaction, cohort, 'commit')
    return True


def clear_global_state():
    global_data.active_transactions = {}
    global_data.active_count = 0
    global_data.transaction_name = ''


if __name__ == '__main__':
    # recovery actions
    app.run()
