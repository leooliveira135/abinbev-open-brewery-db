#!/bin/bash

# enable the sh files to be run
chmod 744 *.sh

# install terraform into the local environment
# this process was made for FEDORA linux distribuiton, here's the doc for linux: https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli
sudo dnf install -y dnf-plugins-core
sudo dnf config-manager --add-repo https://rpm.releases.hashicorp.com/fedora/hashicorp.repo
sudo dnf -y install terraform
terraform -help

# install awscli into the local environment
# this process was made for FEDORA linux distribuiton, here's the doc for linux: https://docs.aws.amazon.com/pt_br/cli/latest/userguide/getting-started-install.html
cd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws --version

# install docker into the local environment
# this process was made for FEDORA linux distribuiton, here's the doc for linux: https://docs.docker.com/desktop/install/linux/
sudo dnf -y install dnf-plugins-core
sudo dnf-3 config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
sudo dnf install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl start docker
sudo docker run hello-world

# fetch the YAML docker compose file for apache airflow
# here's the link for the airflow's documentation used in this process: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.2/docker-compose.yaml'

# now setting up the user airflow and updating the docker image into the environment
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
sudo groupadd docker
sudo gpasswd -a $USER docker
sudo usermod -aG docker $USER
newgrp docker
docker build . -t oliveiraleo135/abinbev-open-brewery-db-java -f Dockerfile
docker run -d oliveiraleo135/abinbev-open-brewery-db-java

# initialize the airflow database migrations and to create the first user account
docker compose up airflow-init

# run the docker compose up to use airflow
docker compose up