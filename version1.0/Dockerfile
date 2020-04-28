FROM puckel/docker-airflow

RUN mkdir /usr/local/airflow/dags
WORKDIR /usr/local/airflow/

COPY ./scripts/python/demo-sample-dag.py ./dags/
RUN python ./dags/demo-sample-dag.py

LABEL maintainer="viswa mohanty <viswa.mohanty@brillio.com>"