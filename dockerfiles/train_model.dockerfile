# Base image
FROM python:3.10.13-slim

RUN apt update && \
    apt install --no-install-recommends -y build-essential gcc && \
    apt clean && rm -rf /var/lib/apt/lists/*

COPY Makefile Makefile
COPY requirements.txt requirements.txt
COPY pyproject.toml pyproject.toml
COPY src/ src/
COPY data/ data/
COPY logging/ logging/
COPY conf/ conf/
COPY README.md README.md

WORKDIR /
RUN pip install -r requirements.txt --no-cache-dir
RUN pip install . --no-deps --no-cache-dir
RUN pip install -e .

# RUN INTERACTIVELY
#ENTRYPOINT ["python", "-u", "src/train_model.py"]