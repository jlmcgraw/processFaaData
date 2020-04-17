FROM ubuntu
RUN apt-get update && apt-get -y install python3 wget sqlite3

RUN mkdir /airport-etl

COPY . /airport-etl

WORKDIR /airport-etl
RUN sh /airport-etl/setup.sh

VOLUME [ "/data" ]

CMD ["sh","/airport-etl/start.sh"]
