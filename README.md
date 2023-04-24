# Project Setup (with Docker)

Run the following commands to get started. We assume you have python 3.0+ set up on your machine.

```python -m pip install kafka-python```

```docker compose -f ./docker-compose-expose.yml up --detach```

Wait for the docker containers to start, then run:

```python ./producer.py```

If all goes well you should see the following output
```
Sent 1 dummy value(s) to log_topic
Sent 2 dummy value(s) to log_topic
Sent 3 dummy value(s) to log_topic
...
```
## Jupyter Notebook Changes

Once the container is running, log into it:

```bash
$ docker exec -it jupyterlab bash
jovyan@1fa4983e08f4:~$ jupyter notebook list
Currently running servers:
http://0.0.0.0:8888/?token=1d6ad10ac4e34e778970115a324617e9e555fde7063f5768 :: /home/jovyan

jovyan@1fa4983e08f4:~$ pip install pyspark==2.4.6
```
