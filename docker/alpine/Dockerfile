# Run this Docker file from exo root directory with: 
# docker build . -f ./docker/alpine/Dockerfile -t exo-alpine:latest --no-cache --progress=plain

# --------------------------
# Step 1 : Build sources
# --------------------------

# Use latest Alpine image as a build image
FROM alpine:latest AS builder

# Install Python 3.12 and other build tools
RUN apk add --no-cache python3 python3-dev py3-pip py3-setuptools git musl-dev linux-headers gcc make g++

# Set the working directory
WORKDIR /app

# Create virtual env
RUN python3 -m venv /opt/exo
ENV PATH="/opt/exo/bin:$PATH"

# Copy sources to current workdir 
COPY . .

# Upgrade pip, setuptools, wheel
# Needed for opencv-python build (more tests needed) 
RUN pip uninstall setuptools
RUN pip install --no-cache-dir --upgrade pip setuptools wheel


# Install exo sources in venv
RUN pip install --no-cache-dir .

# --------------------------
# Step 2 : Build final image
# --------------------------

# Use alpine for final image
FROM alpine:latest AS target

# Install only python3 for runtime
RUN apk add --no-cache python3 uuidgen


# Copy exo build to target image
COPY --from=builder /opt/exo /opt/exo

# Set command to run the application
# NODE_ID will be set with random values, if not defined
CMD /opt/exo/bin/exo --disable-tui --node-id $( [ -z "$NODE_ID" ] && NODE_ID=$(uuidgen); echo $NODE_ID )
