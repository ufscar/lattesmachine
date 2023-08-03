FROM docker.io/library/python:3

RUN apt-get update && apt-get install -y \
  swig \
  libsnappy-dev \
  liblz4-dev \
  && rm -rf /var/lib/apt/lists/*

# from https://github.com/savsgio/docker-rocksdb/blob/main/Dockerfile
ARG ROCKSDB_VERSION=v6.29.5
RUN mkdir -p /usr/src && \
    cd /usr/src && \
    git clone --depth 1 --branch ${ROCKSDB_VERSION} https://github.com/facebook/rocksdb.git && \
    cd /usr/src/rocksdb && \
    make -j4 shared_lib && \
    make install-shared && \
    ldconfig

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY pip-requirements.txt .
RUN pip install -r pip-requirements.txt

COPY lattesmachine ./lattesmachine
ENTRYPOINT ["python", "-m", "lattesmachine"]
