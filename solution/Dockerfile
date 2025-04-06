FROM python:3.12-slim
COPY . /app
WORKDIR /app

ENV POSTGRES_HOST=localhost
ENV POSTGRES_PORT=5432
ENV POSTGRES_USER=postgres
ENV POSTGRES_DB=postgres
ENV CLICKHOUSE_HOST=localhost
ENV CLICKHOUSE_USER=default
ENV CLICKHOUSE_PW=default


RUN pip install --no-cache-dir -r requirements.txt
CMD ["bash"]
