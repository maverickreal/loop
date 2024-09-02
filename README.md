just spawn a docker container for postgresql and rabbitmq and bind it to its default port.
For postgres container, its host, port, username, password, db name are:
```
{
  'NAME': 'loop_db',
  'USER': 'loop_user',
  'PASSWORD': 'loop_password',
  'HOST': '127.0.0.1',
  'PORT': '5432',
}
```
