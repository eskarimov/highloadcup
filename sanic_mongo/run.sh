#! /bin/bash
cd /tmp/data &&
unzip data.zip -d /root/highloadcup &&
cd /root/highloadcup &&
mongod --fork --syslog --dbpath /dev/shm/ &&
python3.6 create_db.py &&
python3.6 sanic_mongo.py