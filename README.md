# MUDROD
## Mining and Utilizing Dataset Relevancy from Oceanographic Dataset to Improve Data Discovery and Access

[![Build Status](https://travis-ci.org/mudrod/mudrod.svg?branch=master)](https://travis-ci.org/mudrod/mudrod)
[![Coverage Status](https://coveralls.io/repos/github/mudrod/mudrod/badge.svg?branch=master)](https://coveralls.io/github/mudrod/mudrod?branch=master)

<img src="http://geant4.slac.stanford.edu/Space06/NASAJPLlogo.jpg" align="right" width="300" />
<img src="https://upload.wikimedia.org/wikipedia/en/thumb/e/e3/GMU_logo.svg/400px-GMU_logo.svg.png" align="right" width="300" />

[MUDROD](https://esto.nasa.gov/forum/estf2015/presentations/Yang_S8P1_ESTF2015.pdf) 
is a semantic discovery and search project funded by NASA AIST (NNX15AM85G).

# Software requirements: 
 * Java 8
 * Git
 * Apache Maven 3.X
 * Elasticsearch v2.6.X
 * Kibana v4
 * Apache Spark v1.6.X
 * Apache Tomcat 8.X

# Installation

## From source
```
$ git clone https://github.com/mudrod/mudrod.git
$ cd mudrod
$ mvn clean install
$ mvn jetty:run
```
You will now be able to access the Mudrod Web Application at http://localhost:8080/mudrod-service

## Deploying to Apache Tomcat (or any other Servlet container)
Once you have built the codebase as above, merely copy the genrated .war artifact to the servlet deployment directory. In Tomcat (for example), this would look as follows
```
$ cp service/target/mudrod-service-0.0.1-SNAPSHOT.war $CATALINA_HOME/webapps/
```
Once Tomcat hot deploys the .war artifact, you will be able to browse to the running application similar to what is shown above http://localhost:8080/mudrod-service

## Docker Container

### Pulling from Dockerhub
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

### Building the Docker Container Manually
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

# Publications

# Documentation

## API Documentation

```
$ mvn javadoc:aggregate
$ open target/site/apidocs/index.html
```

# Team members:

 * Chaowei (Phil) Yang - George Mason University
 * Yongyao Jiang - George Mason University
 * Yun Li - George Mason University
 * Edward M Armstrong - 
 * Thomas Huang - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)
 * David Moroni - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)
 * Chris Finch - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)
 * [Lewis John Mcgibbney](https://www.linkedin.com/in/lmcgibbney) - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)

#License
This source code is licensed under the [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0), a
copy of which is shipped with this project.

