#!/usr/bin/env bash

# Setup Airflow for CentOS 7.6 with 'base' installation
# - Airflow 2.2.5
# - Postgres 9.6 with PyMySQL driver
#

set -o pipefail

export LANG=en_US.UTF-8
export LANGUAGE=en_US.UTF-8
export LC_COLLATE=C
export LC_CTYPE=en_US.UTF-8

export AIRFLOW_VERSION=2.2.5




function set_env_variables {


cat <<EOF > /etc/sysconfig/airflow.env
# config variables
AIRFLOW_HOME=/home/airflow
AIRFLOW_VERSION=${AIRFLOW_VERSION}

AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@127.0.0.1/airflow"
AIRFLOW__CELERY__BROKER_URL="amqp://airflow:airflow@127.0.0.1:5672/"
AIRFLOW__CORE__EXECUTOR="CeleryExecutor"
AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://airflow_scheduler:airflow@127.0.0.1/airflow_celery_db"
AIRFLOW__OPERATORS__DEFAULT_QUEUE="airflow.queue"
AIRFLOW__CORE__FERNET_KEY="q9CkpS4WcYin3YATacDnzZ05bYB1OOdCGV5wnVLqVQM="
AIRFLOW__CORE__LOAD_EXAMPLES=False

PATH=\$PATH:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin
EOF



cat <<EOF > /etc/profile.d/airflow.sh
export AIRFLOW_HOME=/home/airflow
export PATH=\$PATH:/usr/local/bin

export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@127.0.0.1/airflow"
export AIRFLOW__CELERY__BROKER_URL="amqp://airflow:airflow@127.0.0.1:5672/"
export AIRFLOW__CORE__EXECUTOR="CeleryExecutor"
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://airflow_scheduler:airflow@127.0.0.1/airflow_celery_db"
export AIRFLOW__OPERATORS__DEFAULT_QUEUE="airflow.queue"
export AIRFLOW__CORE__FERNET_KEY="q9CkpS4WcYin3YATacDnzZ05bYB1OOdCGV5wnVLqVQM="
export AIRFLOW__CORE__LOAD_EXAMPLES=False
EOF


chmod 777 /etc/profile.d/airflow.sh
source /etc/profile.d/airflow.sh

}

function install_required_libraries() {

  yum -y groupinstall "Development tools"
  yum -y install zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel postgresql-devel python-devel python3-devel libpython3.6-dev gcc wget cyrus-sasl-devel.x86_64 unzip
  yum install python3 -y

  python3 -m pip install --upgrade pip
  python3 -m pip install --upgrade setuptools psycopg2

}

function install_and_start_postgres() {

  yum install -y https://yum.postgresql.org/9.6/redhat/rhel-7.3-x86_64/postgresql96-libs-9.6.11-1PGDG.rhel7.x86_64.rpm
  yum install -y  https://yum.postgresql.org/9.6/redhat/rhel-7.3-x86_64/postgresql96-9.6.11-1PGDG.rhel7.x86_64.rpm
  yum install -y https://yum.postgresql.org/9.6/redhat/rhel-7.3-x86_64/postgresql96-server-9.6.11-1PGDG.rhel7.x86_64.rpm
  yum install -y https://yum.postgresql.org/9.6/redhat/rhel-7.3-x86_64/postgresql96-contrib-9.6.11-1PGDG.rhel7.x86_64.rpm
  yum install -y https://yum.postgresql.org/9.6/redhat/rhel-7.3-x86_64/postgresql96-devel-9.6.11-1PGDG.rhel7.x86_64.rpm

  /usr/pgsql-9.6/bin/postgresql96-setup initdb

  # set local connection to trust in pg_hba.conf
  sed -i 's/host    all             all             127.0.0.1\/32            ident/host    all             all             127.0.0.1\/32            trust/g'  /var/lib/pgsql/9.6/data/pg_hba.conf

  systemctl enable postgresql-9.6.service

  systemctl start postgresql-9.6.service
  systemctl status postgresql-9.6

}

function install_and_start_rabbitmq() {

  yum install epel-release -y
  yum install rabbitmq-server -y

  cat <<EOF > /etc/rabbitmq/rabbitmq-env.conf
NODE_IP_ADDRESS=127.0.0.1
NODENAME=rabbit@localhost
EOF

  export RABBITMQ_CONF_ENV_FILE=/etc/rabbitmq/rabbitmq-env.conf

  rabbitmqctl status
  chkconfig rabbitmq-server on

  ## install rabbitmq web management
  rabbitmq-plugins enable rabbitmq_management

  ## start rabbitmq
  service rabbitmq-server start
  service rabbitmq-server status

  ## create airflow user  set permissions
  rabbitmqctl add_user airflow airflow
  rabbitmqctl set_permissions -p /  airflow ".*" ".*" ".*"

  rabbitmqctl set_user_tags airflow administrator
  wget http://127.0.0.1:15672/cli/rabbitmqadmin
  chmod +x rabbitmqadmin
  sudo ./rabbitmqadmin declare queue name=airflow.queue durable=true

}

function install_and_setup_airflow() {

  PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
  CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
  python3 -m pip install "apache-airflow[async,postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

  python3 -m pip install apache-airflow-providers-celery==2.1.0

  python3 -m pip install flower

  # airflow configuration
  # create airflow user
  useradd -m -r -s /bin/bash "airflow"


  # configure airflow database
  cat <<EOF > /tmp/create-airflow-db-psql.sql
  DROP DATABASE IF EXISTS airflow;
  CREATE DATABASE airflow;
  DROP USER IF EXISTS airflow;
  CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';
  GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
  CREATE DATABASE airflow_celery_db;
  CREATE USER airflow_scheduler WITH ENCRYPTED PASSWORD 'airflow';
  GRANT ALL PRIVILEGES ON DATABASE airflow_celery_db TO airflow_scheduler;
EOF

  sudo -u postgres psql   -f /tmp/create-airflow-db-psql.sql

  mkdir "${AIRFLOW_HOME}"/dags
  mkdir "${AIRFLOW_HOME}"/logs
  mkdir "${AIRFLOW_HOME}"/plugins

  sudo chown -R airflow:airflow "${AIRFLOW_HOME}"

  airflow db init
  airflow users  create --role Admin --username admin --email admin  --password admin
  airflow info
  sudo chown -R airflow:airflow "${AIRFLOW_HOME}"


}

function start_airflow_daemons() {

clear

# create airflow webserver daemon service and start it
cat <<EOF > /tmp/airflow-webserver.service
[Unit]
Description=Airflow webserver daemon

[Service]
User=airflow
Group=airflow
Type=simple
EnvironmentFile=/etc/sysconfig/airflow.env
ExecStart=/bin/bash -c 'airflow webserver -p 8080'
Restart=on-failure
RestartSec=5s
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

sudo mv /tmp/airflow-webserver.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable airflow-webserver.service
sudo systemctl start airflow-webserver.service
sudo systemctl status airflow-webserver.service


# create airflow scheduler daemon service and start it
cat <<EOF > /tmp/airflow-scheduler.service
[Unit]
Description=Airflow scheduler daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow.env
User=airflow
Group=airflow
Type=simple
ExecStart=/bin/bash -c 'airflow scheduler'
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF


sudo mv /tmp/airflow-scheduler.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable airflow-scheduler.service
sudo systemctl start airflow-scheduler.service
sudo systemctl status airflow-scheduler.service




# create airflow celery worker daemon service and start it
cat <<EOF > /tmp/airflow-celery-worker.service
[Unit]
Description=Airflow celery worker daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow.env
User=airflow
Group=airflow
Type=simple
ExecStart=/bin/bash -c 'airflow celery worker'
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
EOF


sudo mv /tmp/airflow-celery-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable airflow-celery-worker.service
sudo systemctl start airflow-celery-worker.service
sudo systemctl status airflow-celery-worker.service


# create airflow flower daemon service and start it
cat <<EOF > /tmp/airflow-celery-flower.service
[Unit]
Description=Airflow celery flower
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow.env
User=airflow
Group=airflow
Type=simple
ExecStart=/bin/bash -c 'airflow celery flower'
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF

sudo mv /tmp/airflow-celery-flower.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable airflow-celery-flower.service
sudo systemctl start airflow-celery-flower.service
sudo systemctl status airflow-celery-flower.service

sudo chown -R airflow:airflow "${AIRFLOW_HOME}"

}

function print_airflow_access_infos() {

    echo "################################################################################################"
    echo "# Installation completed successfully                                                          #"
    echo "#                                                                                              #"
    echo "# airflow webserver UI running on port 8080                                                    #"
    echo "# flower ui running on port 5555                                                               #"
    echo "# rabbitmq running on port 15672                                                               #"
    echo "#                                                                                              #"
    echo "# To start airflow webserver:                                                                  #"
    echo "# sudo systemctl start airflow-webserver.service                                               #"
    echo "#                                                                                              #"
    echo "# To start airflow scheduler:                                                                  #"
    echo "# sudo systemctl start airflow-scheduler.service                                               #"
    echo "#                                                                                              #"
    echo "# To start airflow celery worker:                                                              #"
    echo "# sudo systemctl start airflow-celery-worker.service                                           #"
    echo "#                                                                                              #"
    echo "################################################################################################"



}

function main() {
    set_env_variables
    install_required_libraries
    install_and_start_postgres
    install_and_start_rabbitmq
    install_and_setup_airflow
    start_airflow_daemons
    print_airflow_access_infos
}

main




