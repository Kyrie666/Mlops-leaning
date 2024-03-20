FROM harbor.bielcrystal.com/bieldatalab/py-db-drivers:python3.11-odbc-17

WORKDIR /app

COPY . /app
RUN --mount=type=cache,target=/root/.cache/pip,from=pip_cache pip install -r /app/requirements.txt

RUN chmod +rwx /etc/ssl/openssl.cnf
RUN sed -i 's/TLSv1.2/TLSv1/g' /etc/ssl/openssl.cnf
RUN sed -i 's/SECLEVEL=2/SECLEVEL=1/g' /etc/ssl/openssl.cnf

RUN apt-get install -y fontconfig fonts-wqy-zenhei
RUN fc-cache -fv

EXPOSE 8000
CMD ["uvicorn", "timeseries.main:app", "--host", "0.0.0.0", "--port", "8000"]