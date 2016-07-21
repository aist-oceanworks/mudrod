# Installing MUDROD on CentOS

Background Information
---
This tutorial is designed to enable the user to successfully run MUDROD on the [CentOS](https://www.centos.org/) operating system in a way which does not 
require the use of an IDE.

Running MUDROD on CentOS enables the user to:

1. Reconstruct sessions from user visit logs
2. Mine term relationships based on generated sessions
    

This tutorial and setup will require the use of four main components:

+ Java JDK 1.8
+ Elasticsearch 1.7.3
+ Apache Spark 1.4.0
+ Apache Maven 3.3.9

The installation and configuration of these components will be covered as they are needed to successfully set up MUDROD on CentOS.

Getting MUDROD
---
Going to the main [**MUDROD**](https://github.com/Yongyao/mudrod) github page, download the project as a .zip.  Extract this file to where you would like on your computer.

Installing Java
---
For this tutorial, [**Java JDK 1.8**](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) is required. It can be easily installed by entering the following commands in the terminal.

    $ wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u73-b02/jdk-8u73-linux-x64.rpm"
    $ sudo yum -y localinstall jdk-8u73-linux-x64.rpm

Issue the command `java -version` afterwards to ensure that both Java installed successfully and that the version is correct.

Next, set `JAVA_HOME` to its proper location by issuing the following command after finding the directory where the Java JDK is located:

    $ export JAVA_HOME=/path/to/java/jdk/
    
Installing Elasticsearch 
---
We will be using [**Elasticsearch 1.7.3**](https://www.elastic.co/downloads/past-releases/elasticsearch-1-7-3), an open-source search engine.

First, install the proper version of Elasticsearch by issuing the following command:

    $ sudo yum -y install elasticsearch-1.7.3.noarch.rpm

Next, edit the configuration file located at `/etc/sysconfig/` to improve the query power of Elasticsearch by first opening it with the following command:
    
    $ sudo vi /etc/sysconfig/elasticsearch
    
In the case that you are unfamiliar with vi, press "i" on the keyboard to edit the file.
    
First, find the `#ES_HEAP_SIZE` line and uncomment it, setting it equal to exactly half of the RAM of your computer.  In this example, we have a RAM with a capacity of 4GB

    ES_HEAP_SIZE=2g
    
Next, find the `#MAX_LOCKED_MEMORY` line and uncomment it, leaving it as "unlimited"

    MAX_LOCKED_MEMORY=unlimited
  
This is it for the configuration file.  Save and exit vi by pressing the "Esc" key on the keyboard, then typing `:wq` and pressing enter.  You will be brought back to the command prompt.

We can now start Elasticsearch by issuing the following command 

    $ sudo service elasticsearch start
    
To check if Elasticsearch is successfully running, issue the command `sudo curl 127.0.0.1:9200`, which should return something similar to the following:

```
{
  "status" : 200,
  "name" : "Hyperion",
  "cluster_name" : "elasticsearch",
  "version" : {
    "number" : "1.7.2",
    "build_hash" : "e43676b1385b8125d647f593f7202acbd816e8ec",
    "build_timestamp" : "2015-09-14T09:49:53Z",
    "build_snapshot" : false,
    "lucene_version" : "4.10.4"
  },
  "tagline" : "You Know, for Search"
}
```

Downloading Spark
---
This installation also uses [**Apache Spark 1.4.0**](https://spark.apache.org/releases/spark-release-1-4-0.html), an open-source cluster computing framework.

Download the correct version of Spark by issuing the following command:

    $ wget http://d3kbcqa49mib13.cloudfront.net/spark-1.4.0-bin-hadoop2.4.tgz
    
Next, go to the directory in which Apache Spark downloaded, and unzip it, putting it inside of the folder containing the MUDROD files

    $ tar xvf spark-1.4.0-bin-hadoop2.4.tgz -C /path/to/mudrod/files
    
Getting Maven and Building MUDROD
---
Apache Maven is a build tool for Java projects.  Download Maven 3.3.9 by clicking [this](http://apache.mesi.com.ar/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz).

Next, go to the folder where Maven downloaded to and extract the file in the usual unix way. 

Move the extracted directory to where you would like it.  In this tutorial the Maven file `apache-maven-3.3.9`, it will be located in `/usr/local/apache-maven/`

Now, we need to issue the following xommands to add the appropriate environment variables:

    $ export M2_HOME=/usr/local/apache-maven/apache-maven-3.3.9
    $ export M2=/usr/local/apache-maven/apache-maven-3.3.9/bin
    $ export PATH=$M2:$PATH
    
Finally, verify Maven:

    $ mvn -version
    
Now that we have Maven, we can build MUDROD.  `cd` into the directory where you extracted the MUDROD project and issue the following command:

    $ mvn package
    
This should create a `target` directory containing the appropriate `.jar` file we will use later.

Downloading the Test Data
---
Download all of the test data [here](https://drive.google.com/folderview?id=0B8H4cgkWD42YSS11RjE0UWh5aWM&usp=sharing), making sure to keep track of the directory where it is located after downloading and unzipping if necessary.

Running MUDROD
---
Now that all components have been accounted for, we can run MUDROD on CentOS.  

First, go to the `bin` folder inside of the Apache Spark directory, for example

    $ cd /MUDROD/spark/bin
    
Enter the following command to run MUDROD:

    $ ./spark-submit /path/to/target_folder/mudrod-0.0.1-SNAPSHOT.jar /path/to/test/data -Xss5m
    
The `-Xss5m` argument configures how much stack space this Java program can use.
