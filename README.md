To setup test environment, run the following in shell:

```sh
docker-compose up -d
```
Then in another terminal:
```sh
python manage.py migrate;
python manage.py runserver;
```
# NOTE
The first request you must fire is a get request to `{{host}}:{{port}}/rebuild_datastore/`
Example, `127.0.0.1:8000/rebuild_datastore/`
This will populate the databases using csv files.
