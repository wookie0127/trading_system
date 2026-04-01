# Use a multi-stage build or keep it simple for now
FROM ghcr.io/astral-sh/uv:python3.12-alpine AS builder

# Set the working directory in the container
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy project files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-install-project --no-dev

# Final stage
FROM python:3.12-alpine

WORKDIR /app

# Copy the installed dependencies from the builder
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy the application source code
COPY src/ /app/src/
COPY data/ /app/data/

# Create a logs directory
RUN mkdir -p /app/logs

# Set the working directory to src for convenience
# WORKDIR /app/src

# Default command (can be overridden in docker-compose)
CMD ["python", "src/api.py"]
