To setup test environment, run the following in shell:
## NOTE 1
Kindly create a python2.7 virtual environment, clone the repo inside it, activate the virtual environment, and then run stuff from within `/loop`.
```sh
docker-compose up -d
```
Then in another terminal:
```sh
python manage.py migrate;
python manage.py runserver;
```
## NOTE 1
The first request you must fire is a get request to `{{host}}:{{port}}/rebuild_datastore/`
Example, `GET 127.0.0.1:8000/rebuild_datastore/`
This will populate the databases using csv files.

# NOTE 2
Kindly place the csv files in appropriate directory.
