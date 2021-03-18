# Harvey

Two-phase commit layer on PostgreSQL using Python Flask


## Requirements

Python3, (pip3 install) flask and (pip3 install) psycopg2

## Running the application

FLASK_APP = app.py

FLASK_ENV = development

FLASK_DEBUG = 0

```python -m flask run -p PORT_NUMBER```

### Running multiple cohorts
* Start the harvey-cohort application on different ports using the command above, with corresponding configurations in the ```config.py```.
* Modify the ```config.py``` of the harvey-main application to include the list of cohort ports.

### Firing requests
Send a POST request to harvey-main's /observe with JSON in the following format.
```
{
    "type": "temperature",
    "data": {
        "id": "54fd1b36-84a1-4848-8bcf-cb165b2af686",
        "temperature": 80,
        "timeStamp": "2017-11-08 00:00:00",
        "sensor_id": "30cced27_6cd1_4d82_9894_bddbb71a4401"
    }
}
```
The batch size to be considered as one transaction is configurable in the ```config.py```.
