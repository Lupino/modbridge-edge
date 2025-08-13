FROM python:3.13.6-alpine3.22

WORKDIR /data
COPY requirements.txt /data/requirements.txt
RUN pip3 install -U -r requirements.txt

COPY . /data

CMD ["python3", "/data/dtu.py"]
