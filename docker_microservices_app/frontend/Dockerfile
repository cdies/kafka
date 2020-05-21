FROM python:3.7

RUN python -m pip install confluent-kafka

RUN python -m pip install dash plotly

WORKDIR /app

COPY consumer.py monolith.py ./

CMD ["python", "monolith.py"]