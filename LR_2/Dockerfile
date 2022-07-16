FROM python:3.9-alpine

ARG DEBIAN_FRONTEND=noninteractive

COPY requirements.txt /ot1p/
COPY input/books /ot1p/input/books/
COPY input/voyna-i-mir.txt /ot1p/input/

WORKDIR /ot1p/
RUN pip3 install --no-cache-dir -r requirements.txt

COPY main.py .

ENTRYPOINT ["python3", "main.py"]