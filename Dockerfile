FROM public.ecr.aws/sam/build-python3.12:latest

WORKDIR /var/task

# System deps first — rarely changes, stays cached
RUN dnf install -y openssl && dnf clean all

# Generate self-signed cert
RUN mkdir /etc/ssl/discovery \
    && openssl genrsa -out /etc/ssl/discovery/server.key 2048 \
    && openssl req -new -key /etc/ssl/discovery/server.key -out /etc/ssl/discovery/server.csr \
        -subj '/C=/ST=/L=/O=esgf-west.org/OU=discovery-api/CN=uvicorn' \
    && openssl x509 -req -days 365 -in /etc/ssl/discovery/server.csr \
        -signkey /etc/ssl/discovery/server.key -out /etc/ssl/discovery/server.crt

# Python deps — only re-runs when requirements.txt changes
COPY ./requirements.txt /var/task/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /var/task/requirements.txt

# Vocab downloads — combine into one layer
RUN esgvoc use cmip6@latest \
    && esgvoc use cmip6plus@latest \
    && esgvoc use cordex-cmip6@latest \
    && esgvoc use cmip7@latest \
    && esgvoc use obs4ref@latest \
    && esgvoc use universe@latest

# Source code last — most frequently changed
COPY ./src/stac_fastapi /var/task/stac_fastapi

CMD ["uvicorn", "stac_fastapi.globus_search.app:handler", \
     "--host", "0.0.0.0", "--port", "8000",
     "--forwarded-allow-ips", "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16", \
     "--ssl-keyfile", "/etc/ssl/discovery/server.key", \
     "--ssl-certfile", "/etc/ssl/discovery/server.crt"]
