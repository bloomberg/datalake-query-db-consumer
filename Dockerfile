FROM python:3.9

ARG SQLALCHEMY_DEPENDENCIES=""

WORKDIR /datalakequerydbconsumer

COPY . .

COPY ./log_config.yaml /opt/config/datalakequerydbconsumer/log_config.yaml

RUN python3.9 -m pip install ${SQLALCHEMY_DEPENDENCIES} tox .

ENTRYPOINT [ "python3.9", "-m", "bloomberg.datalake.datalakequerydbconsumer" ]
