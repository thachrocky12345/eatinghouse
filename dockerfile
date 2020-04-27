ARG PYTHON_VERSION=3.7.7-slim

FROM python:${PYTHON_VERSION} AS base

# Turn off debconf messages during build
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

WORKDIR /app

# Install system dependencies
# Docker says run apt-get update and install together,
# and then rm /var/lib/apt/lists to reduce image size.
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils \
    python3-pil \
    python3-requests \
    python3-pip \
    libsm6 libxext6 libxrender-dev python3-pandas python3-matplotlib python3-psycopg2 \
 && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip


# Add requirements.txt before rest of repo, for caching
COPY requirements.txt /
RUN pip3 install -r /requirements.txt

WORKDIR /app
COPY /app /app

ENV DEBIAN_FRONTEND teletype

# Set useful ENV vars
ENV PYTHONIOENCODING "utf-8"

EXPOSE 80

CMD ["python3", "api.py"]

