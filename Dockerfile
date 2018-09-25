FROM ubuntu:16.10

RUN apt-get update && apt-get install -y \
    python-pip \
    python-numpy \
    python-pandas \
    python-protobuf \
    cython \
    wget

RUN pip install pystan fbprophet

ENV KAPACITOR_VERSION 1.4.0
RUN wget https://dl.influxdata.com/kapacitor/releases/python-kapacitor_udf-${KAPACITOR_VERSION}.tar.gz && \
    tar -xvf python-kapacitor_udf-${KAPACITOR_VERSION}.tar.gz && \
    cd kapacitor_udf-${KAPACITOR_VERSION}/ && \
    python setup.py install && \
    cd ../ && \
    rm -rf python-kapacitor_udf-${KAPACITOR_VERSION}.tar.gz kapacitor_udf-${KAPACITOR_VERSION}/

ADD prophet_udf.py /usr/bin/prophet_udf
VOLUME /var/lib/prophet/

ENTRYPOINT ["/usr/bin/prophet_udf", "/var/lib/prophet/prophet.sock"]
