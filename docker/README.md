# MUDROD Dockerfile

[![Docker Pulls](https://img.shields.io/docker/pulls/mudrod/mudrod.svg?maxAge=2592000?style=plastic)](https://hub.docker.com/r/mudrod/mudrod/)
[![Docker Stars](https://img.shields.io/docker/stars/mudrod/mudrod.svg?maxAge=2592000?style=plastic)](https://hub.docker.com/r/mudrod/mudrod/)

# Pulling from Dockerhub
Mudrod is available on Dockerhub for rapid deployment and prototyping.
To get the Mudrod application and environment make sure you have [Docker](https://www.docker.com/) installed then simply execute:
```
$ docker pull mudrod/mudrod
$ docker run -t -i mudrod/mudrod /bin/bash

N.B. If you are on MacOSX you may need to run the following two commands first
$ boot2docker start
$ $(boot2docker shellinit)
```
You can test it by visiting http://container-ip:8080 in a browser or, if you need access outside the host, on port 8888:
```
$ docker run -it --rm -p 8888:8080 mudrod:latest
```

# Building the Docker Container Manually
Simply execute the following
```
$ cd docker
$ docker build -t mudrod/mudrod:latest .
$ docker run -t -i -d --name mudrodcontainer mudrod/mudrod /bin/bash
$ docker attach --sig-proxy=false mudrodcontainer
```
You will now be within the Mudrod environment with all of the tools required to use the software stack, as mentioned in the [Software Requirements](https://github.com/mudrod/mudrod#software-requirements).
You can run mudrod as follows
```
root@e4e137838adc:/usr/local# mudrod
   _____            .___                 .___
  /     \  __ __  __| _/______  ____   __| _/
 /  \ /  \|  |  \/ __ |\_  __ \/  _ \ / __ | 
/    Y    \  |  / /_/ | |  | \(  <_> ) /_/ | 
\____|__  /____/\____ | |__|   \____/\____ | 
        \/           \/                   \/ v0.0.1
Mining and Utilizing Dataset Relevancy from Oceanographic"
Datasets to Improve Data Discovery and Access."
Usage: run COMMAND"
where COMMAND is one of:"
  logingest           ingest logs into Mudrod"
  ...
Most commands print help when invoked w/o parameters."
```

# Support and Development
If you require help with this file, or you have some suggestions
for improving it, please [file an issue](https://github.com/mudrod/mudrod/issues)
and use the **docker** tag.
