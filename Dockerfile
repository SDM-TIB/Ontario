FROM ubuntu:18.04
MAINTAINER Kemele M. Endris <keme686@gmail.com>

USER root

# Python 3.6 and Java 8 installation
RUN apt-get update && \
    apt-get install -y --no-install-recommends nano wget git curl less psmisc && \
    apt-get install -y --no-install-recommends python3.6 python3-pip python3-setuptools && \
    pip3 install --upgrade pip && \
    apt-get install -y --no-install-recommends openjdk-8-jre-headless && \
    apt-get clean

COPY . /Ontario
RUN cd /Ontario && pip3 install -r requirements.txt && \
    python3 setup.py install

RUN mkdir /data
WORKDIR /Ontario

RUN chmod +x /Ontario/start_sparql_service.sh

#CMD ["tail", "-f", "/dev/null"]

CMD ["/Ontario/start_sparql_service.sh"]

