### Mesos slave.
FROM ubuntu:14.04
MAINTAINER Dmitry Platon <platon.dimka@gmail.com>

### Mesos env.
ENV VSN=0.25.0
ENV DIR="/opt/mesos/
ENV WORK_DIR="/opt/mesos/work_dir"

### Install tools.
RUN apt-get install -y wget

### Update the system.
RUN apt-get update

### Install deps.
RUN apt-get install -y openjdk-7-jdk autoconf libtool build-essential python-dev python-boto libcurl4-nss-dev libsasl2-dev maven libapr1-dev libsvn-dev

### Install mesos.
RUN mkdir -p "$DIR" "$WORK_DIR"
RUN cd "$DIR" && wget http://www.apache.org/dist/mesos/"$VSN"/mesos-"$VSN".tar.gz
RUN cd "$DIR" && tar -zxf mesos-"$VSN".tar.gz
RUN cd "$DIR"/mesos-$VSN && mkdir build && cd build && ../configure && make

# CMD ["/bin/bash"]
# CMD ["sh", "-c", "${DIR}/mesos-$VSN/build/bin/mesos-master.sh --work_dir=${WORK_DIR} ${conf}"]
