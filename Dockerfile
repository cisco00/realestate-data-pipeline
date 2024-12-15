FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /database_script

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8089

CMD ["python", "database_script/postgre_scripts.py"]
