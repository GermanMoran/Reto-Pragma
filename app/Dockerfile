FROM python:3.10
# Instalar bash
RUN apt-get update && apt-get install -y bash

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
