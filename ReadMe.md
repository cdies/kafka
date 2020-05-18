1. start kafka:
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh  config/server.properties

2. start backend:
python3 producer.py

3. start frontend:
python3 monolith.py
open browser on page http://127.0.0.1:8050/ 
