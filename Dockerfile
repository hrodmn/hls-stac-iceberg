FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --frozen --no-dev

COPY bootstrap.py .

CMD ["uv", "run", "bootstrap.py"]
