# Build from the repo root (the worker depends on the planner package for the contracts):
#   docker build -f worker/Dockerfile -t ats-worker .
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1

WORKDIR /app
COPY planner/ planner/
COPY worker/ worker/
RUN cd worker && uv sync --frozen --no-dev

ENV PATH="/app/worker/.venv/bin:$PATH"
# Stop with SIGTERM: the worker finishes the attempts it already took, then exits.
STOPSIGNAL SIGTERM
CMD ["python", "-m", "worker"]
