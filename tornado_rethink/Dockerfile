FROM rethinkdb:latest
WORKDIR /root
RUN apt-get update
RUN apt-get install -y unzip wget gcc make build-essential libssl-dev zlib1g-dev memcached locales locales-all parallel python3-dev libmemcached-dev
ENV LANGUAGE ru_RU.UTF-8
ENV LANG ru_RU.UTF-8
ENV LC_ALL ru_RU.UTF-8
RUN locale-gen ru_RU.UTF-8 && dpkg-reconfigure locales
# Install Python
RUN wget https://www.python.org/ftp/python/3.6.2/Python-3.6.2.tgz
RUN tar xvf Python-3.6.2.tgz
RUN cd Python-3.6.2 && ./configure && make -j4 && make altinstall
# update pip
RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install wheel tornado python-dateutil rethinkdb ujson pylibmc rethinkpool asyncio uvloop
# Create folders, add files
RUN mkdir highloadcup
RUN mkdir highloadcup/data
ADD create_db.py highloadcup/
ADD unzip.py highloadcup/
ADD import_memcached.py highloadcup/
ADD tornado_rethink.py highloadcup/
ADD rethinkdb_conf.conf highloadcup/
ADD run.sh highloadcup/
RUN chmod 777 -R highloadcup/
EXPOSE 80
EXPOSE 29015
EXPOSE 28015
EXPOSE 11211
CMD highloadcup/run.sh