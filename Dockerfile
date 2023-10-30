FROM python:3.10

# If this is set to a non-empty string, Python won’t try
# to write .pyc files on the import of source modules
ENV PYTHONDONTWRITEBYTECODE=1

# Force the stdout and stderr streams to be unbuffered.
# This option has no effect on the stdin stream.
ENV PYTHONUNBUFFERED=1

# Install Packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3-dev \
    build-essential \
    # Dependency for psycopg2-binary
    libpq-dev \
    ## cleanup
    && apt-get clean \
    && apt-get autoclean \
    && apt-get autoremove --purge  -y \
    && rm -rf /var/lib/apt/lists/*

# Install python packages
COPY requirements.txt /tmp/pip-tmp/
RUN pip --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
    && rm -rf /tmp/pip-tmp

ARG USERNAME=tnouser
ARG USER_UID=1000
ARG USER_GID=1000

ENV PIPELINE_ROOT="/usr/src/app"
ENV PIPELINE_PATH="/lustre/t1/cl/ton/workflows/pipelines/predict_occultation/pipeline"
ENV PIPELINE_PREDICT_OCC="/lustre/t1/cl/ton/workflows/pipelines/predict_occultation"
ENV PYTHONPATH="/usr/src/app:$PIPELINE_PATH:$PIPELINE_PREDICT_OCC"
ENV WORKFLOW_PATH=$PIPELINE_ROOT
ENV EXECUTION_PATH=$PIPELINE_ROOT

# add user so we can run things as non-root
# https://code.visualstudio.com/remote/advancedcontainers/add-nonroot-user
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && mkdir -p /usr/src/app /log $PIPELINE_PATH $PIPELINE_PREDICT_OCC \
    && chown -R $USER_UID:$USER_GID /usr/src/app /log $PIPELINE_PATH $PIPELINE_PREDICT_OCC

WORKDIR /usr/src/app
COPY . /usr/src/app

COPY --chmod=0775 ./entrypoint.sh /entrypoint.sh
COPY --chmod=0775 ./start.sh /start.sh

# Switch to non-priviliged user and run app
USER tnouser

# NÃO adicionar o script /start.sh no entrypoint
# O /start.sh deve ser adicionado no docker-compose command.
ENTRYPOINT ["/entrypoint.sh"]