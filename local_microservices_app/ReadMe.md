### 2. Local microservices app (Dash, Kafka)

 - start kafka (in Docker) `docker-compose  up -d`

 - start backend `python3 producer.py`

 - start frontend `python3 monolith.py`

 - Go to `http://127.0.0.1:8050/` in browser.