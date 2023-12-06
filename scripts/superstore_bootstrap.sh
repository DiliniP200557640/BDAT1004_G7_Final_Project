#!/bin/bash

sudo yum update -y

sudo yum install gcc-c++ -y
sudo yum list | grep python3
sudo yum install -y python38
sudo ln -s /usr/bin/python3.8 /usr/bin/python --force
sudo yum install -y python pip

sudo python --version
sudo yum install -y python38-pip
sudo yum install -y python38-devel


sudo pip-3.8 install kaggle
sudo pip-3.8 install versioneer
sudo pip-3.8 install Cython==0.29.24
sudo pip-3.8 install numpy==1.20.0
sudo pip-3.8 install python-dateutil
sudo pip-3.8 install pytz
sudo pip-3.8 install pandas==1.3.5
sudo pip-3.8 install boto3
sudo pip-3.8 install psycopg2-binary==2.8.6

sudo mkdir -p /home/ec2-user/superstore/code/
sudo chmod 777 /home/ec2-user/superstore/code/

sudo mkdir ~/.kaggle
sudo chmod 777 ~/.kaggle/

sudo mkdir -p /home/ec2-user/superstore/code/sql/
sudo chmod 777 /home/ec2-user/superstore/code/sql/

sudo mkdir -p /home/ec2-user/superstore/code/config/
sudo chmod 777 /home/ec2-user/superstore/code/config/

aws s3 cp s3://temp-data-dev-2023/code/ /home/ec2-user/superstore/code/ --recursive

sudo aws s3 cp s3://temp-data-dev-2023/code/config/kaggle.json /root/.kaggle/kaggle.json
sudo chmod 600 /root/.kaggle/kaggle.json

sudo ls /home/ec2-user/superstore/code/; 

aws configure set region us-east-1;

