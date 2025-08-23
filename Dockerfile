FROM apache/spark:3.5.0

USER root

RUN wget -P /opt/spark/jars https://jdbc.postgresql.org/download/postgresql-42.7.4.jar