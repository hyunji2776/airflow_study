FROM apache/airflow:2.5.1

USER root

RUN apt-get update && apt-get install -y build-essential
RUN apt-get install -y default-libmysqlclient-dev

CMD ["webserver"]

