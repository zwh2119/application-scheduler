FROM python:3.8
MAINTAINER Wenhui Zhou

COPY ./requirements.txt ./
RUN pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

WORKDIR /app
COPY schedule_server.py scheduler.py pid.py log.py utils.py gunicorn.conf.py /app/


CMD ["gunicorn", "schedule_server:app", "-c", "./gunicorn.conf.py"]