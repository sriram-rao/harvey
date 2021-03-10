import datetime
import string

import requests
from flask import Flask, request

from pgRepo import PostgresRepo

# coordinator for two-phase commit

app = Flask(__name__)
app.config.from_pyfile("config.py")

repo = PostgresRepo(app.config.get('POSTGRES_PORT'))
cohort_ports = list(app.config.get('COHORT_PORTS'))


class Context:
    active_count = 0
    transaction_name = ''
    active_transactions = {}
    transaction_batch = app.config.get('BATCH_SIZE')

    @staticmethod
    def clear_context():
        Context.active_transactions = {}
        Context.active_count = 0
        Context.transaction_name = ''


@app.route('/')
def hello_world():
    return {"response": 'Hello World!'}


@app.route('/observe', methods=['GET', 'POST'])
def insert_observation():
    params = request.get_json()
    data = params['data']
    cohort = cohort_ports[get_hash(data['sensor_id'], data['timeStamp']) % len(cohort_ports)]

    if Context.active_count == 0:
        Context.transaction_name = 'trx'

    if cohort not in Context.active_transactions:
        Context.active_transactions[cohort] = []
    Context.active_transactions[cohort].append(data)
    Context.active_count += 1

    if Context.active_count < Context.transaction_batch:
        return {'result': 'success'}

    result = 'abort'
    try:
        if prepare():
            commit()
            result = 'commit'
        else:
            abort()
            result = 'abort'
    finally:
        Context.clear_context()
        return {'result': f"'{result}'"}


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
    response = requests.post(url, json=data)
    return response.json()['result'] == 'success'


def prepare() -> bool:
    for cohort in cohort_ports:
        data = {"type": "temperature", "transaction": Context.transaction_name,
                "data": Context.active_transactions[cohort]}
        url = f"http://localhost:{cohort}/observe"
        response = requests.post(url, json=data, headers={'Accept': 'application/json',
                                                          'Content-Type': 'application/json'})
        if response.json()['result'] == 'no':
            return False
        repo.log(Context.transaction_name, cohort, 'prepared')
    return True


def abort() -> bool:
    transaction = Context.transaction_name
    responses = {}
    for cohort in cohort_ports:
        repo.log(transaction, cohort, 'abort')
        # abort cohorts
        url = f"http://localhost:{cohort}/abort/{transaction}"
        responses[cohort] = requests.get(url)
        repo.remove_log(transaction, cohort, 'abort')
    return True


def commit() -> bool:
    transaction = Context.transaction_name
    responses = {}
    for cohort in cohort_ports:
        repo.log(transaction, cohort, 'commit')
        # abort cohorts
        url = f"http://localhost:{cohort}/commit/{transaction}"
        responses[cohort] = requests.get(url)
        repo.remove_log(transaction, cohort, 'commit')
    return True


if __name__ == '__main__':
    # recovery actions
    app.run()
