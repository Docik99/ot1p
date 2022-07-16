FROM python:3

ARG DEBIAN_FRONTEND=noninteractive

COPY ./requirements.txt /
RUN pip3 install --no-cache-dir -r requirements.txt

COPY ./cloner /

ENTRYPOINT ["python"]

CMD  []