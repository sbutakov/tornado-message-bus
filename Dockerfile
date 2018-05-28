FROM python:3-alpine
MAINTAINER Sergey Butakov <s.o.butakov@gmail.com>

RUN apk update

COPY requirements.txt /tmp/requirements.txt
COPY message-bus.py /opt/message-bus.py

RUN pip install --no-cache-dir -r /tmp/requirements.txt

EXPOSE 8080
CMD ["/usr/local/bin/python", "/opt/message-bus.py"]