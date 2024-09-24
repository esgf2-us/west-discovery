FROM public.ecr.aws/sam/build-python3.10:latest-x86_64

WORKDIR /var/task

COPY ./requirements.txt /var/task/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /var/task/requirements.txt

COPY ./src/stac_fastapi /var/task/stac_fastapi

CMD ["uvicorn", "stac_fastapi.globus_search.app:handler", "--host", "0.0.0.0", "--port", "8000"]