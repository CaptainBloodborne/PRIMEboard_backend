# start by pulling the python image
FROM python:3.10-slim-buster as dev_build

ARG FAST_API_ENV

# copy the requirements file into the image

#RUN apt-get update && apt-get install -y build-essential cron git curl

ENV FAST_API_ENV=${FAST_API_ENV} \
  # python:
  PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
#  PYTHONHASHSEED=random \
  PYTHONDONTWRITEBYTECODE=1 \
  # pip:
  PIP_NO_CACHE_DIR=1 \
  PIP_DISABLE_PIP_VERSION_CHECK=1 \
  PIP_DEFAULT_TIMEOUT=100 \
#  # dockerize:
#  DOCKERIZE_VERSION=v0.6.1 \
  # poetry:
  POETRY_VERSION=1.2.1 \
  POETRY_NO_INTERACTION=1 \
  POETRY_VIRTUALENVS_CREATE=false \
  POETRY_CACHE_DIR='/var/cache/pypoetry' \
  POETRY_HOME='/usr/local'

RUN apt-get update && apt-get upgrade -y \
  && apt-get install --no-install-recommends -y \
    bash \
    build-essential \
    curl \
    libpq-dev \
    && curl -sSL 'https://install.python-poetry.org' | python - \
    && poetry --version \
  # Cleaning cache:
   && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
   && apt-get clean -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app_backend

COPY ./pyproject.toml ./poetry.lock /app_backend/

RUN --mount=type=cache,target="$POETRY_CACHE_DIR" \
  echo "$FAST_API_ENV" \
  && poetry version \
  # Install deps:
  && poetry run pip install -U pip \
  && poetry install \
    $(if [ "$FAST_API_ENV" = 'production' ]; then echo '--only main'; fi) \
    --no-interaction --no-ansi


RUN pip install --upgrade pip && pip install poetry

#COPY ./requirements.txt /usr/src/app/requirements.txt
#RUN pip install -r requirements.txt


#RUN poetry config virtualenvs.create && poetry install --only main --no-interaction --no-ansi
#
#COPY . /app_backend
#
#
CMD poetry run uvicorn app_backend.main:app --reload --host "0.0.0.0" --port 8000
