#! /bin/bash
export SHELL=/bin/bash
cd /root/highloadcup
# Unzip data
python3.6 unzip.py &
# Start DB
rethinkdb --daemon --config-file rethinkdb_conf.conf &
memcached -u memcache -d -m 2048 &
python3.6 create_db.py &
wait
USER_FILES=/root/highloadcup/data/*users*
LOCATION_FILES=/root/highloadcup/data/*locations*
VISIT_FILES=/root/highloadcup/data/*visits*
# Import to DB
parallel rethinkdb import --force --table travels.users -f ::: $USER_FILES
parallel rethinkdb import --force --table travels.locations -f ::: $LOCATION_FILES
for file in $VISIT_FILES
    do
        rethinkdb import --force --table travels.visits -f $file -c 127.0.0.1:28015 --clients 16
    done
# Import to MC
parallel python3.6 import_memcached.py users ::: $USER_FILES &
parallel python3.6 import_memcached.py locations ::: $LOCATION_FILES &
parallel python3.6 import_memcached.py visits ::: $VISIT_FILES &
# Start Web Server
python3.6 tornado_rethink.py