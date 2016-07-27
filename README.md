# MUDROD
## Mining and Utilizing Dataset Relevancy from Oceanographic Datasets to Improve Data Discovery and Access

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
Please see the [Dockerfile documentation](https://github.com/mudrod/mudrod/tree/master/docker)
for guidance on how to quickly use Docker to deploy Mudrod.

# Publications
Jiang, Y., Y. Li, C. Yang, E. M. Armstrong, T. Huang & D. Moroni (2016) Reconstructing Sessions from Data Discovery and Access Logs to Build a Semantic Knowledge Base for Improving Data Discovery. ISPRS International Journal of Geo-Information, 5, 54. http://www.mdpi.com/2220-9964/5/5/54#stats 
Jiang, Y., Y. Li, C. Yang, K. Liu, E. M. Armstrong, T. Huang & D. Moroni (2016) A Comprehensive Approach to Determining the Linkage Weights among Geospatial Vocabularies - An Example with Oceanographic Data Discovery. International Journal of Geographical Information Science (submitted)
Y. Li, Jiang, Y., C. Yang, K. Liu, E. M. Armstrong, T. Huang & D. Moroni (2016) Leverage cloud computing to improve data access log mining. IEEE Oceans 2016. (in press)


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

