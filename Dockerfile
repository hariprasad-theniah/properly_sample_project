FROM python:3.9
WORKDIR /app/src
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:/app/src"
COPY . .
ENTRYPOINT ["python3"]