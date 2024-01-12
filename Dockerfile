FROM ubuntu:20.04 as base

# Instalação do Anaconda seguindo a imagem oficial
# https://github.com/ContinuumIO/docker-images/blob/main/anaconda3/debian/Dockerfile
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/conda/bin:$PATH

RUN set -x && \
    apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
    gcc \
    bzip2 \
    ca-certificates \
    git \
    libc-dev \
    libglib2.0-0 \
    libpq5 \
    libsm6 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxinerama1 \
    libxrandr2 \
    libxrender1 \
    mercurial \
    openssh-client \
    procps \
    subversion \
    ssh \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* && \
    UNAME_M="$(uname -m)" && \
    if [ "${UNAME_M}" = "x86_64" ]; then \
    ANACONDA_URL="https://repo.anaconda.com/archive/Anaconda3-2023.09-0-Linux-x86_64.sh"; \
    SHA256SUM="6c8a4abb36fbb711dc055b7049a23bbfd61d356de9468b41c5140f8a11abd851"; \
    elif [ "${UNAME_M}" = "s390x" ]; then \
    ANACONDA_URL="https://repo.anaconda.com/archive/Anaconda3-2023.09-0-Linux-s390x.sh"; \
    SHA256SUM="ee817071a2ad94e044fb48061a721bc86606b2f4906b705e4f42177eeb3ca7c5"; \
    elif [ "${UNAME_M}" = "aarch64" ]; then \
    ANACONDA_URL="https://repo.anaconda.com/archive/Anaconda3-2023.09-0-Linux-aarch64.sh"; \
    SHA256SUM="69ee26361c1ec974199bce5c0369e3e9a71541de7979d2b9cfa4af556d1ae0ea"; \
    elif [ "${UNAME_M}" = "ppc64le" ]; then \
    ANACONDA_URL="https://repo.anaconda.com/archive/Anaconda3-2023.09-0-Linux-ppc64le.sh"; \
    SHA256SUM="5ea1ed9808af95eb2655fe6a4ffdb66bea66ecd1d053fc2ee69eacc7685ef665"; \
    fi && \
    wget "${ANACONDA_URL}" -O anaconda.sh -q && \
    echo "${SHA256SUM} anaconda.sh" > shasum && \
    sha256sum --check --status shasum && \
    /bin/bash anaconda.sh -b -p /opt/conda && \
    rm anaconda.sh shasum && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate base" >> ~/.bashrc && \
    find /opt/conda/ -follow -type f -name '*.a' -delete && \
    find /opt/conda/ -follow -type f -name '*.js.map' -delete && \
    /opt/conda/bin/conda clean -afy

# Instalacao do Gfortran 7
RUN apt update \
    && apt install -y gfortran-7 

# -------------- PRAIA OCC compile Stage -------------- 
FROM base as praia_occ
# Instalação do PRAIA OCC
ADD praia_occ_src /tmp/praia_occ_src
RUN mkdir /tmp/praia_occ \
    && cd /tmp/praia_occ_src \
    && gfortran-7 geradata.f -o geradata spicelib.a \
    && mv geradata /tmp/praia_occ \
    && gfortran-7 elimina.f -o elimina \
    && mv elimina /tmp/praia_occ \
    && gfortran-7 PRAIA_occ_star_search_12.f -o PRAIA_occ_star_search_12 \
    && mv PRAIA_occ_star_search_12 /tmp/praia_occ \
    # 	&& gfortran-7 gerapositions.f -o gerapositions \
    # 	&& mv gerapositions /usr/local/bin/ \
    && cd ~/ \
    && rm -r /tmp/praia_occ_src

# -------------- Python 2.7 Environment Stage -------------- 
FROM base as py2_build
# Create the environment
RUN /bin/bash --login -c "conda init bash \
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
    sqlalchemy==1.4.25 \
    psycopg2-binary==2.8.6 \
    && conda deactivate"

# -------------- Python 3.8 Environment Stage -------------- 
FROM base as py3_build
# Create the environment
RUN /bin/bash --login -c "conda init bash \
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
    ## psycopg2==2.8.6 \
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

# Download da BSP planetary
# OBS. o Download demora bastante!
RUN wget --no-verbose --show-progress \
    --progress=bar:force:noscroll \ 
    https://naif.jpl.nasa.gov/pub/naif/generic_kernels/spk/planets/de440.bsp 

# Download Leap Second
RUN wget --no-verbose --show-progress \
    --progress=bar:force:noscroll \
    https://naif.jpl.nasa.gov/pub/naif/generic_kernels/lsk/naif0012.tls 

# PRAIA OCC binaries
COPY --from=praia_occ /tmp/praia_occ/* /usr/local/bin

# Python 2.7 environment
COPY --chown=:conda --chmod=775 --from=py2_build /opt/conda/envs/py2 /opt/conda/envs/py2
# Python 3.8 environment
COPY --chown=:conda --chmod=775 --from=py3_build /opt/conda/envs/py3 /opt/conda/envs/py3

RUN chmod =2775 /opt/conda

# Setup conda to mirror contents from https://github.com/ContinuumIO/docker-images/blob/master/anaconda3/debian/Dockerfile
ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PATH=/opt/conda/bin:$PATH

# devcontainer dependencies and utils 
RUN apt-get update && apt-get install --no-install-recommends -y \
    git \
    bash-completion \
    nano \
    ssh \
    vim \
    rsync \
    wget

ARG APP_HOME=/app

ARG USERNAME=vscode
ARG USERUID=1000
ARG USERGID=1000

# Create remote user 
# Create the conda group and add remote user to the group
RUN groupadd --gid ${USERGID} ${USERNAME} \
    && useradd --uid ${USERUID} --gid ${USERGID} --shell /bin/bash --create-home ${USERNAME} \
    && groupadd --gid 15010 ton \
    && useradd --uid 31670 --gid 15010 --shell /bin/bash --create-home apptno \
    && groupadd -r conda --gid 900 \ 
    && usermod -aG conda,ton ${USERNAME} \
    && usermod -aG conda apptno


ENV PIPELINE_ROOT=${APP_HOME}/src
ENV PIPELINE_PREDIC_OCC=/predict_occultation
ENV PIPELINE_PATH=${PIPELINE_PREDIC_OCC}/pipeline
ENV WORKFLOW_PATH=${PIPELINE_ROOT}
ENV EXECUTION_PATH=${PIPELINE_ROOT}

COPY --chown=${USERNAME}:ton --chmod=775 src ${PIPELINE_ROOT}
COPY --chown=${USERNAME}:ton --chmod=775 predict_occultation ${PIPELINE_PREDIC_OCC}
COPY --chown=${USERNAME}:ton --chmod=775 rerun.py ${APP_HOME}
COPY --chown=${USERNAME}:ton --chmod=775 rerun.sh ${APP_HOME} 
COPY --chown=${USERNAME}:ton --chmod=775 daemon.sh ${APP_HOME}
COPY --chown=${USERNAME}:ton --chmod=775 entrypoint.sh ${APP_HOME}
COPY --chown=${USERNAME}:ton --chmod=775 run_daemon.py ${APP_HOME}

RUN chmod =2775 ${PIPELINE_ROOT}
RUN chmod =2775 ${APP_HOME}

RUN mv de440.bsp ${PIPELINE_PATH}/de440.bsp \
    && mv naif0012.tls ${PIPELINE_PATH}/naif0012.tls

RUN rm -rf ${PIPELINE_ROOT}/*.pyc ${PIPELINE_ROOT}/__pycache__
RUN rm -rf ${PIPELINE_ROOT}/external_inputs/*.pyc ${PIPELINE_ROOT}/external_inputs/__pycache__
RUN rm -rf ${PIPELINE_PATH}/*.pyc ${PIPELINE_PATH}/__pycache__
RUN rm -rf ${PIPELINE_PREDICT_OCC}/*.pyc ${PIPELINE_PREDIC_OCC}/__pycache__

WORKDIR ${APP_HOME}

USER apptno
RUN /bin/bash --login -c "conda init bash \
    && echo 'conda activate py3' >> ~/.bashrc \
    && source ~/.bashrc"

USER ${USERNAME}
RUN /bin/bash --login -c "conda init bash \
    && echo 'conda activate py3' >> ~/.bashrc \
    && source ~/.bashrc"

ENTRYPOINT [ "./entrypoint.sh" ]
