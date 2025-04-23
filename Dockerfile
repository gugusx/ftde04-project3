#Dockerfile
FROM apache/airflow:2.8.3
# Install additional dependencies
USER root
USER airflow
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    pymysql \
    pymongo \
    psycopg2-binary \
    pandas \
    gspread \
    sqlalchemy \
    oauth2client==4.1.3