FROM apache/airflow:2.2.3

USER airflow
RUN pip3 install openpyxl 
USER root
COPY --chown=airflow:root ./dags/raizen_challenge_dag.py /opt/airflow/dags/ 
USER airflow

