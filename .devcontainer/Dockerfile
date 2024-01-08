FROM mcr.microsoft.com/devcontainers/python:0-3.8

# If this is set to a non-empty string, Python won’t try
# to write .pyc files on the import of source modules
ENV PYTHONDONTWRITEBYTECODE=1

# Force the stdout and stderr streams to be unbuffered.
# This option has no effect on the stdin stream.
ENV PYTHONUNBUFFERED=1

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    gcc \
    python3-dev \
    build-essential \
    libpcre3 \
    libpcre3-dev \
    pip \
    && apt-get clean \
    && apt-get autoclean \
    && apt-get autoremove --purge  -y \
    && rm -rf /var/lib/apt/lists/*

RUN pip install \
    parsl==2023.9.25 \
    numpy==1.21.2 \
    astropy==5.0.3 \
    astroquery==0.4.6 \
    humanize==3.10.0 \
    pandas==1.4.1 \
    psycopg2==2.8.6 \
    python-dateutil==2.8.2 \
    scipy==1.7.3 \
    requests==2.27.1 \
    spiceypy==5.0.1 \
    sqlalchemy==1.4.32 

COPY --chmod=0775 ./entrypoint.sh /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]