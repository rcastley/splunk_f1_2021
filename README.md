# README

Check out this repository and change into the directory `splunk_f1_2021`

```
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
python3 main.py
```

If you get an error `socket.gaierror: [Errno 8] No nodename or servername provided, or not known` add the following to your `/etc/hosts` file where `[YOURHOSTNAME]` is the hostname of your machine

```
0.0.0.0     [YOURHOSTNAME]
```
