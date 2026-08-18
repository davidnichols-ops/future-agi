# Slim on purpose: the harness needs Python, Node (the Agent SDK drives the
# Claude Code CLI as a subprocess), a docker *client*, and six packages. It
# never imports Django, so the 17.8 GB backend image buys nothing here.
# 3.11 to match Dockerfile.base across the stack.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates gnupg \
 && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
 && install -m 0755 -d /etc/apt/keyrings \
 && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
 && echo "deb [signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" \
      > /etc/apt/sources.list.d/docker.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends nodejs docker-ce-cli git \
 && npm install -g @anthropic-ai/claude-code \
 && apt-get purge -y gnupg && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

# artifacts/ resolves relative to the working directory; the volume mounts here.
WORKDIR /app

COPY pyproject.toml LICENSE NOTICE ./
COPY src ./src
COPY ui ./ui
RUN pip install --no-cache-dir -e .[ui,postgres]

ENV HARNESS_HOST=0.0.0.0
EXPOSE 8777 8787
CMD ["python", "ui/server.py"]
