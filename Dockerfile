FROM python:3.8
MAINTAINER Wenhui Zhou

COPY ./requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

CMD ["gunicorn", "schedule_server:app", "-c", "./gunicorn.conf.py"]