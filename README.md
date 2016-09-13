# MUDROD
## Mining and Utilizing Dataset Relevancy from Oceanographic Datasets to Improve Data Discovery and Access

[![license](https://img.shields.io/github/license/mudrod/mudrod.svg?maxAge=2592000?style=plastic)](http://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://travis-ci.org/mudrod/mudrod.svg?branch=master)](https://travis-ci.org/mudrod/mudrod)
[![Docker Pulls](https://img.shields.io/docker/pulls/mudrod/mudrod.svg?maxAge=2592000?style=plastic)](https://hub.docker.com/r/mudrod/mudrod/)
[![codecov](https://codecov.io/gh/mudrod/mudrod/branch/master/graph/badge.svg)](https://codecov.io/gh/mudrod/mudrod)
[![SonarQube Tech Debt](https://img.shields.io/sonar/http/nemo.sonarqube.org/esiptestbed:mudrod-parent/tech_debt.svg?maxAge=2592000?style=plastic)](http://nemo.sonarqube.org/dashboard/index/esiptestbed:mudrod-parent)

<img src="http://geant4.slac.stanford.edu/Space06/NASAJPLlogo.jpg" align="right" width="300" />
<img src="http://www.iucrc.org/sites/default/files/centerLogo.png" align="right" width="170" />

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

## Docker Container
We strongly advise all users to save time and effort by consulting the [Dockerfile documentation](https://github.com/mudrod/mudrod/tree/master/docker)
for guidance on how to quickly use Docker to deploy Mudrod.

## From source
Ensure you have Elasticsearch running locally.
```
$ git clone https://github.com/mudrod/mudrod.git
$ cd mudrod
$ mvn clean install
$ cd service
$ mvn jetty:run
```
You will now be able to access the Mudrod Web Application at [http://localhost:8080/mudrod-service](http://localhost:8080/mudrod-service). **N.B.** The service should not be run this way in production. Please see below for running the service via Tomcat.

In another window...
```
$ cd mudrod
$ ./core/target/appassembler/bin/mudrod
usage: MudrodEngine: 'logDir' argument is mandatory. User must also
       provide either 'logIngest' or 'fullIngest'. [-f] [-h] [-l] -logDir
       <logDir>
 -f,--fullIngest    begin full ingest Mudrod workflow
 -h,--help          show this help message
 -l,--logIngest     begin log ingest with the WeblogDiscoveryEngine only
 -logDir <logDir>   the log directory to be processed by Mudrod
MudrodEngine: 'logDir' argument is mandatory. User must also provide either 'logIngest' or 'fullIngest'.
```

## Deploying to Apache Tomcat (or any other Servlet container)
Once you have built the codebase as above, merely copy the genrated .war artifact to the servlet deployment directory. In Tomcat (for example), this would look as follows
```
$ cp mudrod/service/target/mudrod-service-${version}-SNAPSHOT.war $CATALINA_HOME/webapps/
```
Once Tomcat hot deploys the .war artifact, you will be able to browse to the running application similar to what is shown above [http://localhost:8080/mudrod-service](http://localhost:8080/mudrod-service)

# Publications
* Jiang, Y., Y. Li, C. Yang, E. M. Armstrong, T. Huang & D. Moroni (2016) Reconstructing Sessions from Data Discovery and Access Logs to Build a Semantic Knowledge Base for Improving Data Discovery. ISPRS International Journal of Geo-Information, 5, 54. http://www.mdpi.com/2220-9964/5/5/54#stats 
* Jiang, Y., Y. Li, C. Yang, K. Liu, E. M. Armstrong, T. Huang & D. Moroni (2016) A Comprehensive Approach to Determining the Linkage Weights among Geospatial Vocabularies - An Example with Oceanographic Data Discovery. International Journal of Geographical Information Science (submitted)
* Y. Li, Jiang, Y., C. Yang, K. Liu, E. M. Armstrong, T. Huang & D. Moroni (2016) Leverage cloud computing to improve data access log mining. IEEE Oceans 2016. (in press)


# Documentation

## API Documentation

```
$ mvn javadoc:aggregate
$ open target/site/apidocs/index.html
```

# Team members:

 * Chaowei (Phil) Yang - [NSF Spatiotemporal Innovation Center](http://stcenter.net/), George Mason University
 * [Yongyao Jiang](https://www.linkedin.com/in/yongyao-jiang-42516164) - [NSF Spatiotemporal Innovation Center](http://stcenter.net/), George Mason University
 * Yun Li - [NSF Spatiotemporal Innovation Center](http://stcenter.net/), George Mason University
 * Edward M Armstrong - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)
 * Thomas Huang - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)
 * David Moroni - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)
 * Chris Finch - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)
 * [Lewis John Mcgibbney](https://www.linkedin.com/in/lmcgibbney) - [Jet Propulsion Laboratory](http://www.jpl.nasa.gov/), [NASA](http://www.nasa.gov)

#License
This source code is licensed under the [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0), a
copy of which is shipped with this project.

