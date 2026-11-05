FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root
RUN pip3 install --no-cache-dir psycopg2-binary==2.9.10 pymongo==4.11.3 \
    && python3 -c "from urllib.request import urlretrieve; urlretrieve('https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar', '/opt/spark/jars/postgresql.jar')"
USER spark
WORKDIR /app
