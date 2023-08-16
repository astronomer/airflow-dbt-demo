FROM apache/airflow:2.6.3
ADD requirements.txt . 
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt 