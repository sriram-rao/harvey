import datetime
import requests
from flask import Flask, request
from pgRepo import PostgresRepo
import string
import uuid

# coordinator for two-phase commit

app = Flask(__name__)
app.config.from_pyfile("config.py")
repo = PostgresRepo(app.config.get('POSTGRES_PORT'))
cohort_ports = list(app.config.get('COHORT_PORTS'))

def recover():
    transaction, status = repo.get_last_status()
    Context.transaction_name = transaction
    action = 'commit' if status == 'to-commit' else 'abort'
    if complete_transaction(action):
        repo.log(Context.transaction_name, 0, 'complete')
        repo.remove_transaction(Context.transaction_name)
    Context.clear()


recover()

class Context:
    active_count = 0
    transaction_name = ''
    active_transactions = {}
    transaction_batch = app.config.get('BATCH_SIZE')

    @staticmethod
    def clear():
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
        Context.transaction_name = f"trx-{uuid.uuid4()}"

    if cohort not in Context.active_transactions:
        Context.active_transactions[cohort] = []
    Context.active_transactions[cohort].append(data)
    Context.active_count += 1

    if Context.active_count < Context.transaction_batch:
        return {'result': 'success'}

    action = 'commit' if prepare() else 'abort'
    result = action if complete_transaction(action) else 'incomplete'
    repo.log(Context.transaction_name, 0, 'complete')
    repo.remove_transaction(Context.transaction_name)
    Context.clear()
    return {'result': f"{result}"}


@app.route('/status/<name>/<cohort>')
def get_status(name: string, cohort: int):
    return {'status': f"{repo.get_status(name, cohort)}"}


@app.route('/register')
def register_status():
    cohort = request.args.get('cohort')
    name = request.args.get('name')
    action = request.args.get('action')
    repo.remove_log(name, cohort, action)
    if repo.is_complete(name):
        repo.log(name, 0, 'complete')
    return {'result': 'success'}

def get_hash(sensor: string, time: datetime) -> int:
    hash_string = f"{sensor}_{time}"
    return hash_string.__hash__()


def begin_transaction(cohort: int, name: string):
    url = f"http://localhost:{cohort}/begin/{name}"
    response = requests.get(url)
    return response.json()['result'] == 'success'


def post_data(port: int, data: dict) -> bool:
    url = f"http://localhost:{port}/observe"
    response = requests.post(url, json=data)
    return response.json()['result'] == 'success'


def prepare() -> bool:
    name = Context.transaction_name
    for cohort in cohort_ports:
        data = {"type": "temperature", "transaction": name,
                "data": Context.active_transactions[cohort]}
        url = f"http://localhost:{cohort}/observe"
        response = requests.post(url, json=data, headers={'Accept': 'application/json',
                                                          'Content-Type': 'application/json'})
        if response.json()['result'] == 'no':
            index = cohort_ports.index(cohort)
            for i in range(0, index):
                repo.log(name, cohort, 'abort')
            repo.log(name, 0, 'to-abort')
            return False
        repo.log(name, cohort, 'prepared')
    for cohort in cohort_ports:
        repo.log(name, cohort, 'commit')
    repo.log(name, 0, 'to-commit')
    return True


def complete_transaction(action: string):
    name = Context.transaction_name
    responses = {}
    complete = True
    for cohort in cohort_ports:
        try:
            url = f"http://localhost:{cohort}/{action}/{name}"
            responses[cohort] = requests.get(url)
            repo.remove_log(name, cohort, action)
        except Exception as e:
            complete = False
            continue
    return complete


if __name__ == '__main__':
    app.run()
