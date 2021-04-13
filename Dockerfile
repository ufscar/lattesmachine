FROM docker.io/library/pypy:3

RUN apt-get update && apt-get install -y \
  swig \
  librocksdb-dev \
  libsnappy-dev \
  liblz4-dev \
  && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/opt/venv
RUN pypy3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY pip-requirements.txt .
RUN pip install -r pip-requirements.txt

COPY lattesmachine ./lattesmachine
ENTRYPOINT ["python", "-m", "lattesmachine"]
