# Use a multi-stage build or keep it simple for now
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

# Set the working directory in the container
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy project files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-install-project --no-dev

# Final stage
# Keep the runtime base aligned with the builder so the copied .venv
# keeps matching Python shared libraries and ABI paths.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl git ca-certificates \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && npm install -g @google/gemini-cli \
    && rm -rf /var/lib/apt/lists/*

# Copy the installed dependencies from the builder
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy the application source code
COPY pyproject.toml uv.lock prefect.yaml README.md /app/
COPY src/ /app/src/

# Create a logs directory
RUN mkdir -p /app/data /app/logs /app/obsidian

# Set the working directory to src for convenience
# WORKDIR /app/src

# Default command (can be overridden in docker-compose)
CMD ["python", "src/api.py"]
