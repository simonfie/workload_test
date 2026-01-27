A simple celery workload testing code

# Usage

Create and use a virtual environment in all terminals

`python -m venv venv`
`source venv/bin/activate`
`pip install -r requirements.txt`


`source venv/bin/activate`

1. Start the Django server: `python manage.py runserver`
2. Start the main celery worker from /backend: `celery -A app worker --loglevel=INFO `
3. Start flower monitor from /backend: `celery -A app flower`
4. Run the testing script: `python test.py`
+ To run the custom monitor for task events: `python monitor.py`