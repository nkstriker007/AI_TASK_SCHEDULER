# Build from the repo root (the API depends on the planner package):
#   docker build -f api/Dockerfile -t ats-api .
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1

WORKDIR /app
COPY planner/ planner/
COPY api/ api/
RUN cd api && uv sync --frozen --no-dev

ENV PATH="/app/api/.venv/bin:$PATH"
WORKDIR /app/api
EXPOSE 8000
CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]
