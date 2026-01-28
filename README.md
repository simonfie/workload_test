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
2. Start the Django server: `python manage.py runserver`
3. Start the main celery worker from /backend: `celery -A app worker --loglevel=INFO`
4. Start flower monitor from /backend: `celery -A app flower`
5. Run the testing script: `python test.py`
+ To run the custom monitor for task events: `python monitor.py`