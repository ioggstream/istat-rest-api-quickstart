FROM python:3.7
COPY . /src
WORKDIR /src
RUN pip install tox poetry
ENTRYPOINT ["tox"]

