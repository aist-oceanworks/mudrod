# MUDROD
## Mining and Utilizing Dataset Relevancy from Oceanographic Datasets to Improve Data Discovery and Access

[![license](https://img.shields.io/github/license/mudrod/mudrod.svg?maxAge=2592000?style=plastic)](http://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://travis-ci.org/mudrod/mudrod.svg?branch=master)](https://travis-ci.org/mudrod/mudrod)
[![Docker Pulls](https://img.shields.io/docker/pulls/mudrod/mudrod.svg?maxAge=2592000?style=plastic)](https://hub.docker.com/r/mudrod/mudrod/) [![](https://images.microbadger.com/badges/image/mudrod/mudrod.svg)](https://microbadger.com/images/mudrod/mudrod "Get your own image badge on microbadger.com")
[![codecov](https://codecov.io/gh/mudrod/mudrod/branch/master/graph/badge.svg)](https://codecov.io/gh/mudrod/mudrod)
[![Quality Gate](https://sonarqube.com/api/badges/gate?key=gov.nasa.jpl.mudrod:mudrod-parent)](https://sonarqube.com/dashboard/index/gov.nasa.jpl.mudrod:mudrod-parent)

<img src="http://geant4.slac.stanford.edu/Space06/NASAJPLlogo.jpg" align="right" width="300" />
<img src="http://www.iucrc.org/sites/default/files/centerLogo.png" align="right" width="170" />

[MUDROD](https://esto.nasa.gov/forum/estf2015/presentations/Yang_S8P1_ESTF2015.pdf) 
is a semantic discovery and search project funded by NASA AIST (NNX15AM85G).

# Software requirements: 
 * Java 8
 * Git
 * Apache Maven 3.X
 * Elasticsearch v5.X
 * Kibana v4
 * Apache Spark v2.0.0
 * Apache Tomcat 7.X

# Installation

## Docker Container
We strongly advise all users to save time and effort by consulting the [Dockerfile documentation](https://github.com/mudrod/mudrod/tree/master/docker)
for guidance on how to quickly use Docker to deploy Mudrod.

## From source
1. Ensure you have Elasticsearch running locally and that the configuration in [config.xml](https://github.com/mudrod/mudrod/blob/master/core/src/main/resources/config.xml) reflects your ES cluster.
2. Update the `svmSgdModel` configuration option in [config.xml](https://github.com/mudrod/mudrod/blob/master/core/src/main/resources/config.xml). There is a line in config.xml that looks like 
    ```
    <para name="svmSgdModel">file://YOUNEEDTOCHANGETHIS</para>
    ```
    It needs to be changed to an absolute filepath on your system. For example:
    ```
    <para name="svmSgdModel">file:///Users/user/githubprojects/mudrod/core/src/main/resources/javaSVMWithSGDModel</para>
    ```
3. (Optional) Depending on your computer's configuration you might run into an error when starting the application: `“Service 'sparkDriver' could not bind on port 0”`. The easiest [fix](http://stackoverflow.com/q/29906686/953327) is to export the environment variable `SPARK_LOCAL_IP=127.0.0.1 ` and then start the service.

```
$ git clone https://github.com/mudrod/mudrod.git
$ cd mudrod
$ mvn clean install
$ cd service
$ mvn tomcat7:run
```
You will now be able to access the Mudrod Web Application at [http://localhost:8080/mudrod-service](http://localhost:8080/mudrod-service). **N.B.** The service should not be run this way in production.

# Documentation

In another window...
```
$ cd mudrod
$ ./core/target/appassembler/bin/mudrod-engine -h
usage: MudrodEngine: 'logDir' argument is mandatory. User must also
       provide an ingest method. [-a] [-esHost <host_name>] [-esPort
       <port_num>] [-esTCPPort <port_num>] [-f] [-h] [-l] -logDir
       </path/to/log/directory> [-p] [-s] [-v]
 -a,--addSimFromMetadataAndOnto                          begin adding
                                                         metadata and
                                                         ontology results
 -esHost,--elasticSearchHost <host_name>                 elasticsearch
                                                         cluster unicast
                                                         host
 -esPort,--elasticSearchHTTPPort <port_num>              elasticsearch
                                                         HTTP/REST port
 -esTCPPort,--elasticSearchTransportTCPPort <port_num>   elasticsearch
                                                         transport TCP
                                                         port
 -f,--fullIngest                                         begin full ingest
                                                         Mudrod workflow
 -h,--help                                               show this help
                                                         message
 -l,--logIngest                                          begin log ingest
                                                         without any
                                                         processing only
 -logDir,--logDirectory </path/to/log/directory>         the log directory
                                                         to be processed
                                                         by Mudrod
 -p,--processingWithPreResults                           begin processing
                                                         with
                                                         preprocessing
                                                         results
 -s,--sessionReconstruction                              begin session
                                                         reconstruction
 -v,--vocabSimFromLog                                    begin similarity
                                                         calulation from
                                                         web log Mudrod
                                                         workflow
```

## Deploying to Apache Tomcat (or any other Servlet container)
Once you have built the codebase as above, merely copy the genrated .war artifact to the servlet deployment directory. In Tomcat (for example), this would look as follows
```
$ cp mudrod/service/target/mudrod-service-${version}-SNAPSHOT.war $CATALINA_HOME/webapps/
```
Once Tomcat hot deploys the .war artifact, you will be able to browse to the running application similar to what is shown above [http://localhost:8080/mudrod-service](http://localhost:8080/mudrod-service)

# Publications
* Jiang, Y., Li, Y., Yang, C., Liu, K., Armstrong, E.M., Huang, T., Moroni, D.F. and Finch, C.J., 2017. [A comprehensive methodology for discovering semantic relationships among geospatial vocabularies using oceanographic data discovery as an example](http://www.tandfonline.com/eprint/JgGYJBk4mhdt5NFjEx8f/full). International Journal of Geographical Information Science, pp.1-19.
* Jiang, Y., Y. Li, C. Yang, K. Liu, E. M. Armstrong, T. Huang, D. Moroni & L. Mcgibbney (2016) Towards intelligent geospatial discovery: a machine learning ranking framework (Accepted). International Journal of Digital Earth
* Jiang, Y., Y. Li, C. Yang, E. M. Armstrong, T. Huang & D. Moroni (2016) [Reconstructing Sessions from Data Discovery and Access Logs to Build a Semantic Knowledge Base for Improving Data Discovery](http://www.mdpi.com/2220-9964/5/5/54#stats ). ISPRS International Journal of Geo-Information, 5, 54.
* Y. Li, Jiang, Y., C. Yang, K. Liu, E. M. Armstrong, T. Huang & D. Moroni (2016) Leverage cloud computing to improve data access log mining. IEEE Oceans 2016.

## Mudrod Wiki

https://github.com/mudrod/mudrod/wiki

## Java API Documentation

```
$ mvn javadoc:aggregate
$ open target/site/apidocs/index.html
```

## REST API Documentation

```
$ mvn clean install
$ open service/target/miredot/index.html
```
The REST API documentation can also be seen at [https://mudrod.github.io/miredot](https://mudrod.github.io/miredot).

# Team members:

 * Chaowei (Phil) Yang - [NSF Spatiotemporal Innovation Center](http://stcenter.net/), George Mason University
 * [Yongyao Jiang](https://www.linkedin.com/in/yongyao-jiang-42516164) - [NSF Spatiotemporal Innovation Center](http://stcenter.net/), George Mason University
 * Yun Li - [NSF Spatiotemporal Innovation Center](http://stcenter.net/), George Mason University
 * Edward M Armstrong - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)
 * Thomas Huang - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)
 * David Moroni - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)
 * Chris Finch - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)
 * [Lewis John Mcgibbney](https://www.linkedin.com/in/lmcgibbney) - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)
 * [Frank Greguska](https://www.linkedin.com/in/frankgreguska/) - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)
 
# License
This source code is licensed under the [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0), a
copy of which is shipped with this project. 
