FROM mongo:latest
WORKDIR /root
RUN apt-get update
RUN apt-get install -y unzip wget gcc make build-essential libssl-dev zlib1g-dev python-dev
RUN wget https://www.python.org/ftp/python/3.6.2/Python-3.6.2.tgz
RUN tar xvf Python-3.6.2.tgz
RUN cd Python-3.6.2 && ./configure && make -j4 && make altinstall
# update pip
RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install wheel sanic python-dateutil motor
RUN mkdir highloadcup
ADD sanic_mongo.py highloadcup/
ADD create_db.py highloadcup/
ADD run.sh highloadcup/
RUN chmod 777 highloadcup/run.sh
EXPOSE 80
EXPOSE 27017
CMD highloadcup/run.sh
