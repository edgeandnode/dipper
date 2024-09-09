## Rust builder
# Compile the Rust code and link against the system libpython3-dev
# The libpython3-dev package version must match the final image's python version
FROM rust:1.81.0-bookworm AS rust-builder

# Install dependencies
#  - libpython3-dev:
#        Required for building the pyo3 crate
#        For debian bookworm, python3-dev version is 3.11.2-1
#        https://packages.debian.org/bookworm/libpython3-dev
RUN apt-get update \
  && apt-get install -y \
      build-essential \
      clang \
      cmake \
      git \
      libpython3-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY ./ ./

ENV CC=clang CXX=clang++
RUN cargo build --bin dipper-service --release

## Python builder
# Package the python code (sdist)
FROM python:3.12-bookworm AS python-builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /src
COPY ./ ./

RUN uv build --sdist

## Final image
FROM python:3.12-slim-bookworm

# Install dependencies
RUN apt-get update \
  && apt-get install -y ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Set uv environment variables
#  - Use the system python
ENV UV_SYSTEM_PYTHON=1
#  - Don't create a virtual environment (.venv) when syncing
ENV UV_PROJECT_ENVIRONMENT=""
#  - Copy packages from the global cache into the site-packages directory
ENV UV_LINK_MODE=copy
#  - Compile Python files to bytecode after installation
ENV UV_COMPILE_BYTECODE=1

WORKDIR /app

# Install python dependencies
RUN --mount=from=ghcr.io/astral-sh/uv,source=/uv,target=/usr/local/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --frozen --no-install-project --no-dev

# Install the iisa package
RUN --mount=from=ghcr.io/astral-sh/uv,source=/uv,target=/usr/local/bin/uv \
    --mount=from=python-builder,source=/src/dist,target=/src/dist \
    uv pip install --system /src/dist/*.tar.gz

# Install the dipper-service binary
COPY --from=rust-builder /src/target/release/dipper-service /usr/local/bin/dipper-service

ENTRYPOINT [ "dipper-service" ]
