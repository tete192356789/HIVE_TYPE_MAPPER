FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    wget \
    unzip \ 
    software-properties-common \
    git \
    libaio1 \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip3 install -r requirements.txt


#RUN mkdir /opt/oracle
#COPY instantclient-basic-linux.x64-21.15.0.0.0dbru.zip /opt/oracle

#RUN unzip instantclient-basic-linux.x64-21.15.0.0.0dbru.zip
RUN sh -c "echo /app/instantclient_21_15 > /etc/ld.so.conf.d/oracle-instantclient.conf"
ENV LD_LIBRARY_PATH=/app/instantclient_21_15:$LD_LIBRARY_PATH
RUN ldconfig

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
