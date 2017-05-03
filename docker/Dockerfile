FROM ubuntu:16.04

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927
RUN echo "deb http://repo.mongodb.org/apt/ubuntu $(cat /etc/lsb-release | grep DISTRIB_CODENAME | cut -d= -f2)/mongodb-org/3.2 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.2.list

RUN apt-get update && apt-get install -y wget vim net-tools unzip default-jre git python-pip mongodb-org 

RUN mkdir -p /data/db

RUN pip install --pre pybasex voluptuous bottle httplib2 pymongo nose-exclude

WORKDIR /opt
RUN wget http://files.basex.org/releases/8.6.2/BaseX862.zip
RUN unzip BaseX862.zip && rm BaseX862.zip

RUN git clone https://github.com/crs4/pyEHR.git
WORKDIR /opt/pyEHR
RUN python setup.py install

WORKDIR /opt
COPY launch_services /usr/local/bin
RUN mkdir /etc/pyehr
COPY services.mongodb.conf /etc/pyehr/

ENV PATH=$PATH:/opt/basex/bin
ENV EXCLUDE_TEST_MODULE="elasticsearch|.*performance*"
ENV SERVICE_CONFIG_FILE=/etc/pyehr/services.mongodb.conf

EXPOSE 27017 8984 8985 8080 8090

ENTRYPOINT ["/usr/local/bin/launch_services"]
