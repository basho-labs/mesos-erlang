### Mesos slave.
FROM ubuntu:14.04
MAINTAINER Dmitry Platon <platon.dimka@gmail.com>

### Mesos env.
ENV VSN=0.28.1
ENV DIR="/opt/mesos/"
ENV FRAMEWORKS_HOME_DIR="/opt/mesos/frameworks_home"

### Install tools.
RUN apt-get install -y wget

### Update the system.
RUN apt-get update

### Install deps.
RUN apt-get install -y openjdk-7-jdk autoconf libtool build-essential python-dev python-boto libcurl4-nss-dev libsasl2-dev maven libapr1-dev libsvn-dev

### Install mesos.
RUN mkdir -p "$DIR" "FRAMEWORKS_HOME_DIR"
RUN cd "$DIR" && wget http://www.apache.org/dist/mesos/"$VSN"/mesos-"$VSN".tar.gz
RUN cd "$DIR" && tar -zxf mesos-"$VSN".tar.gz
RUN mv "$DIR"/mesos-$VSN "$DIR"/mesos
RUN cd "$DIR"/mesos && mkdir build && cd build && ../configure && make

### Copy frameworks.
COPY frameworks_home/ "$FRAMEWORKS_HOME_DIR"

CMD ["sh", "-c", "${DIR}/mesos/build/bin/mesos-slave.sh --resources='cpus:0.5' --isolation=process --attributes=isolation:process --frameworks_home=${FRAMEWORKS_HOME_DIR} ${conf}"]
