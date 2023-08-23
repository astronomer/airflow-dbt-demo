FROM apache/airflow:2.6.3
USER root
RUN apt-get update && \
    apt-get install -y gnupg2 gcc g++ lsb-release libpq-dev libsasl2-modules libsasl2-dev python3-dev
USER ${AIRFLOW_UID}
ADD requirements.txt . 
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt 