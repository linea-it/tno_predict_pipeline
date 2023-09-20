FROM python:3.8-slim

ENV PYTHONUNBUFFERED 1

# If this is set to a non-empty string, Python won’t try
# to write .pyc files on the import of source modules
ENV PYTHONDONTWRITEBYTECODE=1


COPY requirements.txt /tmp/pip-tmp/
RUN pip3 --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
   && rm -rf /tmp/pip-tmp

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <your-package-list-here>

ARG USERNAME=tno
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN mkdir /usr/src/app /log  \
    && groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && chown -R $USER_UID:$USER_GID /usr/src/app /log

WORKDIR /usr/src/app
COPY . /usr/src/app

USER $USERNAME