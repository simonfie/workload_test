A simple celery workload testing code

# Requiremets

python 3.10
rabbimtq-server


# Usage

Create and use a virtual environment in all terminals

`python -m venv venv`

`source venv/bin/activate`

`pip install -r requirements.txt`


`source venv/bin/activate`



1. Start the rabbitmq-server: `sudo systemctl start rabbitmq-server`
   - check the status of the server with: `sudo systemctl status rabbitmq-server`
2. Create a postgres database called `naboso` owned by user `user` with password `password`
3. Make migrations: `python manage.py makemigrations`
4. Migrate: `python manage.py migrate`
5. Start the Django server: `python manage.py runserver`
6. Install minio (linux install):
   - `wget https://dl.min.io/server/minio/release/linux-amd64/minio`
   - `chmod +x minio`
   - `sudo mv minio /usr/local/bin/`
7. Start minio server and open on `localhost:9001` with minioadmin as name an password: `minio server /tmp/minio-data --console-address ":9001"`
8. Here create a bucket called `test-bucket`
9.  Start the main celery worker from /backend: `celery -A app worker --loglevel=INFO`
10. Start the external worker from /backend: `celery -A external_worker.template worker --loglevel=INFO -Q template -n template`
11. Run the testing script: `python test.py [id]`
+ To see the progress of the tasks: `python progress.py [id]`
+ Start flower monitor from /backend: `celery -A app flower`


# Notes
For the progress monitoring, make sure to input the same id as for the test.py script

The standalone_task4 fails on purpose to test error logging

monitor.py was the first attempt at progress logging and is not being used as of now

logger is also set to update the log row in JobTask table

To update state, steps, message of a task, use update_state() as in tasks.py