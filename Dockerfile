FROM apache/airflow:2.8.3-python3.11

USER root

# Install Microsoft ODBC driver for SQL Server into Debian 12 Bookworm
RUN apt-get update && apt-get install -y curl apt-transport-https gnupg2 \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc > /etc/apt/trusted.gpg.d/microsoft.asc \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Fix OpenSSL 3.x rejecting legacy SSL algorithms from older SQL Server instances
# Create a standalone config with the full reference chain so OpenSSL actually reads it
RUN printf '\
openssl_conf = openssl_init\n\
[openssl_init]\n\
ssl_conf = ssl_sect\n\
[ssl_sect]\n\
system_default = system_default_sect\n\
[system_default_sect]\n\
MinProtocol = TLSv1\n\
CipherString = DEFAULT@SECLEVEL=0\n' > /etc/ssl/openssl_allow_legacy.cnf

ENV OPENSSL_CONF=/etc/ssl/openssl_allow_legacy.cnf

USER airflow

# Install python dependencies for your pipeline
RUN pip install --no-cache-dir pyodbc pandas google-cloud-bigquery pandas-gbq dbt-bigquery python-dotenv
