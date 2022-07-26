# README

Check out this repository and change into the directory `splunk_f1_2021`

```
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
python3 main.py
```

if you get an error socket.gaierror: [Errno 8] nodename nor servname provided, or not known

add the following to your /etc/hosts file wher [YOURHOSTNAME] is the host name of your machine
0.0.0.0     [YOURHOSTNAME]