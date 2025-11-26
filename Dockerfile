FROM ubuntu:22.04 AS builder

# avoid stuck build due to user prompt
ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Create pipeline user here so venv paths line up
RUN useradd --create-home --uid 1000 pipeline

RUN apt-get update && apt-get install --no-install-recommends -y \
    python3.11 python3.11-dev python3-pip python3.11-venv \
    build-essential \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

USER pipeline
WORKDIR /home/pipeline

RUN python3.11 -m venv /home/pipeline/venv
ENV PATH="/home/pipeline/venv/bin:$PATH"

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

FROM ubuntu:22.04 AS app-runner

RUN apt-get update && apt-get install --no-install-recommends -y \
    python3.11 python3.11-dev python3-venv \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Create pipeline user here so venv paths line up
RUN useradd --create-home --uid 1000 pipeline

COPY --from=builder /home/pipeline/venv /home/pipeline/venv

USER pipeline
RUN mkdir /home/pipeline/code
WORKDIR /home/pipeline/code

# Create dirs (important BEFORE mounting volumes)
RUN mkdir -p input output logs cache tmp
RUN chmod -R 775 /home/pipeline/code

COPY . .

EXPOSE 8787

# activate virtual environment
ENV VIRTUAL_ENV=/home/pipeline/venv
ENV PATH="/home/pipeline/venv/bin:$PATH"

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python3.11 -c "import sys; sys.exit(0)" || exit 1

ENTRYPOINT ["python3.11", "run_pipeline.py"]
CMD ["--json-root", "input", "--db-path", "output/leads.db"]