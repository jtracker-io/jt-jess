FROM python:3.7

# Metadata
LABEL base.image="python:3.7"
LABEL software="JTracker Job Execution Scheduling Service"
LABEL website="https://github.com/jtracker-io/jt-jess"

# File Author / Maintainer
MAINTAINER Junjun Zhang <junjun.ca@gmail.com>

USER root

RUN wget -O tmp.tar.gz -t 10 https://github.com/jtracker-io/jt-jess/archive/0.2.0a25.tar.gz && \
    mkdir -p /srv/jt-jess && \
    tar -C /srv/jt-jess --strip-components 1 -zxf tmp.tar.gz && \
    pip3 install -r /srv/jt-jess/requirements.txt

CMD ["python3", "/srv/jt-jess/app.py"]
