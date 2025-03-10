# Run this Docker file from exo root directory with:
# docker build . -f ./docker/debian/Dockerfile -t exo:debian-latest
# 
# find . -type d -name __pycache__ -exec rm -r {} \+
# rm -fr build
# docker system prune
# exit venv

# --------------------------
# Step 1 : Build sources
# --------------------------

# Use bookworm-slim image as a build image
FROM debian:bookworm-20250224-slim AS builder

# Install build tools
RUN apt-get update
#RUN apt-get install -y software-properties-common 
RUN apt-get install -y python-is-python3 python3-pip python3-venv git

# Set the working directory
WORKDIR /app


# Copy sources to current workdir
COPY . .

# Create virtual env
RUN python -m venv /opt/exo
ENV PATH="/opt/exo/bin:$PATH"

# Upgrade pip, setuptools, wheel
# llvmlite -> Needed but not in setup.py ?
RUN pip install --no-cache-dir --upgrade pip setuptools wheel llvmlite

# Install exo sources in venv
RUN pip install --no-cache-dir .

# --------------------------
# Step 2 : Build final image
# --------------------------

# Use bookworm-slim for final image
FROM debian:bookworm-20250224-slim AS target

# Install minimal packages required for exo runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends python-is-python3 libgl1 libglib2.0-dev uuid-runtime clang wget curl

# Copy exo build to target image
COPY --from=builder /opt/exo /opt/exo

# Clean the image
RUN apt-get autoremove \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set command to run the application
# NODE_ID will be set with random values, if not defined
CMD /opt/exo/bin/exo --disable-tui --node-id $( [ -z "$NODE_ID" ] && NODE_ID=$(uuidgen); echo $NODE_ID )
