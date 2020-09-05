FROM ubuntu

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get -y install python3 wget sqlite3 build-essential 
RUN mkdir /faa

COPY . /faa

WORKDIR /faa
RUN sh /faa/setup.sh

VOLUME [ "/data" ]

CMD ["sh","/faa/start.sh"]
