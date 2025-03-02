# Run this Docker file from exo root directory with:
# docker build . -f ./docker/arch/Dockerfile -t exo-arch:latest --no-cache --progress=plain

# --------------------------
# Step 1 : Build sources
# --------------------------

# Use bookworm-slim image as a build image
FROM archlinux:base-devel AS builder

# Install build tools
RUN pacman -Sy
RUN pacman -S python python-pip git --noconfirm

# Set the working directory
WORKDIR /app

# Create virtual env
RUN python -m venv /opt/exo
ENV PATH="/opt/exo/bin:$PATH"

# Copy sources to current workdir
COPY . .

# Upgrade pip, setuptools, wheel
# Needed for opencv-python build (more tests needed)
#RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install exo sources in venv
RUN pip install --no-cache-dir .

# --------------------------
# Step 2 : Build final image
# --------------------------

# Use bookworm-slim for final image
FROM debian:bookworm-20250224-slim AS target

# Install only python3 for runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    uuid-runtime \
    && rm -rf /var/lib/apt/lists/*

# Copy exo build to target image
COPY --from=builder /opt/exo /opt/exo

# Set command to run the application
# NODE_ID will be set with random values, if not defined
CMD /opt/exo/bin/exo --disable-tui --node-id $( [ -z "$NODE_ID" ] && NODE_ID=$(uuidgen); echo $NODE_ID )
