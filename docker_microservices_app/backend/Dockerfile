FROM python:3.7

RUN python -m pip install confluent-kafka

RUN python -m pip install pyorbital

WORKDIR /app

COPY producer.py ./

CMD ["python", "producer.py"]