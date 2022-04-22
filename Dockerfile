FROM python:3

RUN mkdir -p /app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		ca-certificates \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/*

COPY [ "cloudinventario", "/app" ]
COPY [ "requirements.txt", "/app" ]

WORKDIR "/app"
RUN [ "pip", "install", "-r", "requirements.txt" ]

COPY [ "src", "/app/src" ]

#ENV PYTHONPATH "/app/src"

VOLUME /conf /conf
VOLUME /data /data

# By default we wait for docker exec
CMD [ "tail", "-f", "/dev/null" ]
