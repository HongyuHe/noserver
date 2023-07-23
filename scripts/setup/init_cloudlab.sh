#! /bin/bash

sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install moreutils -y
sudo apt-get install python3.10 -y
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 2
sudo apt install python3-apt -y
sudo apt install python3.10-distutils -y

curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10
sudo cp /users/hy/.local/bin/pip /usr/bin/pip
pip install -r requirements.txt

