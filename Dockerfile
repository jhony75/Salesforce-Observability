FROM python:3.12-slim

WORKDIR /app
COPY . /app/

RUN pip install prometheus-client
RUN pip install simple-salesforce
RUN pip install python-dotenv

EXPOSE 8000

CMD python -u ./main.py