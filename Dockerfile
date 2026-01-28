FROM public.ecr.aws/lambda/python:3.12

WORKDIR /var/task

ENV PYTHONPATH="/var/task"

COPY pyproject.toml poetry.lock ./

COPY historical_flights_airport_gym/ ./historical_flights_airport_gym

RUN python3 -m pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --only main --no-root
