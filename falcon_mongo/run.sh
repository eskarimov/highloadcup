#! /bin/bash
cd /tmp/data &&
unzip data.zip -d /root/highloadcup &&
cd /root/highloadcup &&
mongod --fork --syslog --dbpath /dev/shm/ &&
python3.6 create_db.py &&
gunicorn falcon_mongo:api -b 0.0.0.0:80 -k gevent -w 4 --worker-connections=2000