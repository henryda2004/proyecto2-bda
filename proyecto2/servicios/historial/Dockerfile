FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV HISTORIAL_DB=/data/historial.db
ENV DATA_LAKE=/datalake/eventos
ENV BATCH_SIZE=1000

EXPOSE 8003

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8003"]
