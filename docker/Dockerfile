FROM ubuntu:16.04

# Metadata
LABEL base.image="ubuntu:16.04"
LABEL software="JTracker Job Execution Scheduling Service"
LABEL website="https://github.com/jthub/jt-jess"

# File Author / Maintainer
MAINTAINER Junjun Zhang <junjun.ca@gmail.com>

USER root

RUN apt-get -yqq update && \
    apt-get -yqq install curl

RUN curl -sSL -o tmp.tar.gz --retry 10 https://github.com/jthub/jt-jess/archive/0.2.0a9.tar.gz && \
    mkdir -p /srv/jt-jess && \
    tar -C /srv/jt-jess --strip-components 1 -zxf tmp.tar.gz && \
    apt-get -yqq install python3-pip && \
    pip3 install --upgrade pip  && \
    pip3 install -r /srv/jt-jess/requirements.txt

CMD ["python3", "/srv/jt-jess/app.py"]
