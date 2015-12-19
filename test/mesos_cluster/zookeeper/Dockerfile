### Zookeeper.
FROM ubuntu:14.04
MAINTAINER Dmitry Platon <platon.dimka@gmail.com>

ENV VSN=3.4.6
ENV DIR="/opt/zk"
ENV DATA_DIR="data"
ENV LOG_DIR="log"

### Update the system.
RUN apt-get update

### Install tools.
RUN apt-get install -y wget

### Install deps.
RUN apt-get install -y openjdk-7-jdk

### Install zookeeper.
RUN mkdir -p "$DIR" && cd "$DIR" && wget http://archive.apache.org/dist/zookeeper/zookeeper-"$VSN"/zookeeper-"$VSN".tar.gz
RUN cd "$DIR" && tar -zxf zookeeper-"$VSN".tar.gz && mv zookeeper-"$VSN"/* ./ && rm -rf zookeeper-"$VSN".tar.gz zookeeper-"$VSN"/

RUN mkdir -p "$DIR"/"$DATA_DIR" "$DIR"/"$LOG_DIR"

COPY start_server.sh "$DIR"/start_server.sh
RUN chmod a+x "$DIR"/start_server.sh

CMD ["sh" , "-c", "${DIR}/start_server.sh"]
