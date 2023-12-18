FROM continuumio/anaconda3 as base

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    gcc \
    gfortran-9 \
    vim

# -------------- PRAIA OCC compile Stage -------------- 
FROM base as praia_occ
# Instalação do PRAIA OCC
ADD aux/praia_occ_src /tmp/praia_occ_src
RUN mkdir /tmp/praia_occ \
    && cd /tmp/praia_occ_src \
    && gfortran-9 geradata.f -o geradata spicelib.a \
    && mv geradata /tmp/praia_occ \
    && gfortran-9 elimina.f -o elimina \
    && mv elimina /tmp/praia_occ \
    && gfortran-9 PRAIA_occ_star_search_12.f -o PRAIA_occ_star_search_12 \
    && mv PRAIA_occ_star_search_12 /tmp/praia_occ \
    # 	&& gfortran-9 gerapositions.f -o gerapositions \
    # 	&& mv gerapositions /usr/local/bin/ \
    && cd /tmp/praia_occ \
    && wget --no-verbose --show-progress \
    --progress=bar:force:noscroll \ 
    https://naif.jpl.nasa.gov/pub/naif/generic_kernels/spk/planets/de440.bsp \
    &&  wget --no-verbose --show-progress \
    --progress=bar:force:noscroll \
    https://naif.jpl.nasa.gov/pub/naif/generic_kernels/lsk/naif0012.tls \
    && rm -r /tmp/praia_occ_src

# -------------- Python 2.7 Environment Stage -------------- 
FROM base as py2_build
# Create the environment
RUN /bin/bash -c "conda init bash \
    && source ~/.bashrc \
    && conda create -y -n py2 python=2.7 \
    && conda activate py2 \
    && conda install pip freetype libpng pkg-config \
    && pip install \
    numpy==1.16.6 \
    astropy==2.0.8 \
    matplotlib \
    Pillow==5.3.0 \
    OWSLib==0.17.0 \
    Cython==0.29 \
    pyproj==1.9.5.1 \
    pyshp==1.2.12 \
    pandas==0.24.2 \
    spiceypy==2.1.2 \
    && conda deactivate"

# -------------- Python 3.8 Environment Stage -------------- 
FROM base as py3_build
# Create the environment
RUN /bin/bash -c "conda init bash \
    && source ~/.bashrc \
    && conda create -y -n py3 python=3.8 \
    && conda activate py3 \
    && conda install pip \ 
    && pip install \
    parsl==2023.9.25 \
    numpy==1.21.2 \
    astropy==5.0.3 \
    astroquery==0.4.6 \
    humanize==3.10.0 \
    pandas==1.4.1 \
    psycopg2==2.8.6 \
    psycopg2-binary==2.8.6 \
    python-dateutil==2.8.2 \
    scipy==1.7.3 \
    requests==2.27.1 \
    spiceypy==5.0.1 \
    sqlalchemy==1.4.32 \
    celery==5.3 \
    && conda deactivate"

# -------------- Runtime Stage -------------- 
FROM base

ARG USERNAME=tno
ARG USERUID=1000
ARG USERGID=1000

# Create remote user 
# Create the conda group and add remote user to the group
RUN groupadd --gid 1000 ${USERNAME} \
    && useradd --uid ${USERUID} --gid ${USERGID} --shell /bin/bash --create-home ${USERNAME} \
    && groupadd -r conda --gid 900 \ 
    && usermod -aG conda ${USERNAME}
#   && echo dev-user ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/dev-user \
#   && chmod 0440 /etc/sudoers.d/dev-user

# PRAIA OCC binaries
COPY --from=praia_occ /tmp/praia_occ/* /usr/local/bin

# Python 2.7 environment
COPY --chown=:conda --chmod=775 --from=py2_build /opt/conda/envs/py2 /opt/conda/envs/py2
# Python 3.8 environment
COPY --chown=:conda --chmod=775 --from=py3_build /opt/conda/envs/py3 /opt/conda/envs/py3

RUN chmod =2775 /opt/conda

# devcontainer dependencies and utils 
RUN apt-get update && apt-get install --no-install-recommends -y \
    sudo git bash-completion nano ssh 

ARG APP_HOME=/app
COPY . /app
RUN chown -R ${USERUID}:${USERGID} /app
WORKDIR ${APP_HOME}

ENV PYTHONPATH="$APP_HOME:$PYTHONPATH"

USER ${USERNAME}

# Setup conda to mirror contents from https://github.com/ContinuumIO/docker-images/blob/master/anaconda3/debian/Dockerfile
ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PATH=/opt/conda/bin:$PATH

RUN /bin/bash -c "conda init bash \
    && echo 'conda activate py3' >> ~/.bashrc \
    && source ~/.bashrc"

COPY --chmod=0775 ./run_parsl.sh /run_parsl.sh
COPY --chmod=0775 ./entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
