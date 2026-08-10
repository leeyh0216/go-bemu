# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
# This is intentionally a client-only image: it never embeds the emulator
# binary and reaches the Compose service only through its public HTTP endpoint.
FROM python:3.13-slim@sha256:69e18bd8d831d88e0ef70239dc7771ab7c28bc296ae78ac75cde71e60aa4434f

WORKDIR /workspace
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY tests/integration/python/requirements.lock /tmp/requirements.lock
RUN python -m pip install --no-cache-dir --require-hashes -r /tmp/requirements.lock

# The normalized consumer manifest owns this exact wheel URI and digest. ADD's
# checksum keeps the image build itself as strict as the host runner.
ADD --checksum=sha256:a39217f14f215472ce9da816f20ebaf77fdb1db7ccdc8360772d8bf6bafb55c2 \
  https://files.pythonhosted.org/packages/67/c0/f491506bdff4d73750f74cf18fea7d6ca409ada2f81c4c4d10a32edf5688/google_cloud_bigquery-3.43.0-py3-none-any.whl \
  /tmp/google_cloud_bigquery-3.43.0-py3-none-any.whl
RUN python -m pip install --no-cache-dir /tmp/google_cloud_bigquery-3.43.0-py3-none-any.whl

COPY tests ./tests
COPY contract/operations.normalized.json ./contract/operations.normalized.json

ENTRYPOINT ["python", "-m", "pytest"]
