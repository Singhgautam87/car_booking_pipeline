FROM jenkins/jenkins:lts
USER root

RUN apt-get update && apt-get install -y \
    docker.io \
    curl \
    git \
    python3 \
    python3-pip \
 && rm -rf /var/lib/apt/lists/*

RUN curl -L https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-linux-x86_64 \
    -o /usr/local/bin/docker-compose && chmod +x /usr/local/bin/docker-compose

# ✅ Jenkins user ko docker group mein add karo
RUN usermod -aG docker jenkins

USER jenkins