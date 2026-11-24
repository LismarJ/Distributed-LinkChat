# One image, three roles. The role is the first argument to the container.
#
# Base image choice is not final: the alternative is `rayproject/ray:<ver>-py311-cpu`,
# which is larger but saves installing Ray. The spike decides, by measuring
# image size and node start time (ADR pending, technology-stack.md section 4).
#
# Python 3.12 because Ray supports 3.10 to 3.13. It cannot run on 3.14, which
# is what the development machines have as their system Python.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10

# procps gives pgrep and pkill, which the fault-injection scripts use to kill a
# relay's worker process from inside its container without killing the node.
# libsodium is for PyNaCl. The X3DH library (ADR-0009) needs nothing here: its
# XEdDSA dependency ships manylinux wheels with the native code inside.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      procps \
      libsodium23 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies first, so editing our own code does not reinstall Ray.
#
# The stub package is installed to pull in the dependencies, then uninstalled.
# Without that uninstall, pip would see linkchat 0.0.0 already present and skip
# the real install below, leaving the image running an empty package.
# The stub sits at src/linkchat because pyproject.toml packages that path.
COPY pyproject.toml README.md ./
RUN mkdir -p src/linkchat \
 && touch src/linkchat/__init__.py \
 && pip install . \
 && pip uninstall -y linkchat \
 && rm -rf src

COPY src ./src
COPY spike ./spike
RUN pip install --no-deps .

COPY docker/entrypoint.sh /usr/local/bin/linkchat
RUN chmod +x /usr/local/bin/linkchat

ENTRYPOINT ["/usr/local/bin/linkchat"]
CMD ["help"]
