
FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir poetry==2.5.1

COPY pyproject.toml poetry.lock ./

ENV POETRY_VIRTUALENVS_CREATE=false

RUN poetry install --only main --no-root --no-interaction --no-ansi

COPY . .