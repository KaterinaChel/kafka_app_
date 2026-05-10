FROM python:3.9

RUN apt-get update && apt-get install -y ca-certificates && update-ca-certificates
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org \
    confluent-kafka[avro] certifi fastavro requests \
    faust-streaming==0.10.4 \
    redis==5.0.1
    
#COPY filter_service.py /app/filter_service.py
#COPY client_producer.py /app/client_producer.py
#COPY shop_producer.py /app/shop_producer.py
#COPY spark_etl_job.py /app/spark_etl_job.py
#CMD ["python", "/app/shop_producer.py"]
#CMD ["python", "/app/client_producer.py"]
#CMD ["python", "/app/filter_service.py"]
#CMD ["python", "/app/spark_etl_job.py"]