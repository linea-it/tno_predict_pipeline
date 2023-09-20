FROM mcr.microsoft.com/devcontainers/python:0-3.8

ENV PYTHONUNBUFFERED 1

# If this is set to a non-empty string, Python won’t try
# to write .pyc files on the import of source modules
ENV PYTHONDONTWRITEBYTECODE=1

COPY tno/requirements.txt /tmp/pip-tmp/
RUN pip3 --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
   && rm -rf /tmp/pip-tmp

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <your-package-list-here>



