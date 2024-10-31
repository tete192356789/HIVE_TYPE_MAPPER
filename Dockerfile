FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    wget \
    unzip \ 
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/tete192356789/HIVE_TYPE_MAPPER.git .

RUN pip3 install -r requirements.txt

RUN wget https://download.oracle.com/otn_software/linux/instantclient/2115000/instantclient-basic-linux.x64-21.15.0.0.0dbru.zip
RUN mkdir /opt/oracle && mv instantclient-basic-linux.x64-21.15.0.0.0dbru.zip /opt/oracle && unzip instantclient-basic-linux.x64-21.15.0.0.0dbru.zip
RUN sudo sh -c "echo /opt/oracle/instantclient_21_15 > /etc/ld.so.conf.d/oracle-instantclient.conf"
RUN export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_15:$LD_LIBRARY_PATH
RUN sudo ldconfig

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]