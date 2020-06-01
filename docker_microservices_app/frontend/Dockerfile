FROM python:3.7

RUN python -m pip install confluent-kafka

RUN python -m pip install dash plotly

WORKDIR /app

COPY consumer.py graph_display.py ./

CMD ["python", "graph_display.py"]