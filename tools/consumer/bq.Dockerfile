# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
# The digest and Cloud SDK version are locked by the normalized bq CLI case.
FROM gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:80a5d770cea35cd01cf233e066115421930336afdec3716c930db5c653ea6aa5

WORKDIR /workspace

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY tests ./tests
COPY contract/operations.normalized.json ./contract/operations.normalized.json

ENTRYPOINT ["python3", "tests/integration/bqcli/run_contract.py"]
