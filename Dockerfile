# syntax=docker/dockerfile:1.7
#
# Build:
#   docker build -t binance-cash-and-carry-scanner .
#
# Run (defaults to --help so the container exits cleanly without hitting Binance):
#   docker run --rm binance-cash-and-carry-scanner
#
# Run the actual scanner (terminal dashboard):
#   docker run --rm -it binance-cash-and-carry-scanner \
#       --no-startup-alert --base-pair BTCUSDT
#

FROM python:3.12-slim AS runtime

# Hygiene flags for Python in containers
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install runtime dependencies first so this layer is cached when only
# application code changes.
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy the application code.
COPY binance_carry/ ./binance_carry/
COPY multicac.py cac_btc.py ./

# Drop privileges: run as an unprivileged user.
RUN useradd --create-home --shell /bin/bash scanner \
    && chown -R scanner:scanner /app
USER scanner

ENTRYPOINT ["python", "-m", "binance_carry"]

# Default to --help so `docker run image` (no args) shows usage and exits cleanly.
# Override at runtime with: docker run image --base-pair BTCUSDT --no-startup-alert
CMD ["--help"]