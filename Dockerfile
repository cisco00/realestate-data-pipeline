FROM python:3.10

WORKDIR /database_script

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "database_script/postgre_scripts.py"]
