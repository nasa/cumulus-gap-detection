FROM public.ecr.aws/lambda/python:3.13
WORKDIR /app

COPY test/requirements.txt /app/test_requirements.txt
RUN pip install -U -r test_requirements.txt

COPY test/ /app/test/
COPY src/ /app/src/
COPY src/shared/gap_schema.sql /app/

ENTRYPOINT []
