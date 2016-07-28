# MUDROD GUI Guide

Background Information
---
This tutorial is designed to enable the user to successfully use web service of MUDROD.

MUDROD GUI enables the user to:

1. Explore imported logs
2. Learn session structure and details
3. Interact with vacobulary ontology graph
4. Find dataset using semantic search
    

This tutorial will require MUDROD already published as a web service, please refer to [**Installing MUDROD on CentOS**](https://github.com/mudrod/mudrod/blob/master/Installation.md)

Mudrod provides two kinds of web services. One web service is based on Kibana, a visulization tool of ELK stack. The port of this service is 5601. Another one is  java web service developed on own own, which is published through tomcat and the port is 8080.

# Log Exploration:
---


First, visit the homepage of kibana:

    http://your_public_ip:5601/

> At the first time you visit this page, the system will let  you  configure an index pattern. Please input the index name in the "index name or pattern" field, such as mudrod. And then choose the Time-field name.
![](https://raw.githubusercontent.com/quintinali/images/master/add%20index.png)
> After clicking the create button, all fileds in  the index will be listed. Please find a filed named "SessionURL" and edit it as following. 
1. Change format to URL
2. Input "view" in label template and save it.
![](https://raw.githubusercontent.com/quintinali/images/master/edit%20index.png)

Next, choose "Discover" tab at the menu bar. 

![](https://raw.githubusercontent.com/quintinali/images/master/discover%20default.png)

If the system tells you no result is found, please change time range by clicking the topright area. The following graph shows how to set time range from 2014-01-01 to 2014-12-31 using absolute value.

![](https://raw.githubusercontent.com/quintinali/images/master/change%20time%20range.png)

Once the time range is set, logs falling into the range show in the main area of the discover page.

![](https://raw.githubusercontent.com/quintinali/images/master/defaul%20log%20list.png)

If you are interested  in data visulaization and want to explore the log data deeply, please refer to [**Kibana User Guide**](https://www.elastic.co/guide/en/kibana/4.1/index.html)


# Session Structure
---


First, visit the homepage of kibana as we described in Log Exploration part.

Next, input "sessionstats*" in the search box. The table below the histogram will list data in types with name containing "sessionstats".
![](https://raw.githubusercontent.com/quintinali/images/master/improved%20log%20list.png)

In order to get a brief overview of  sessions, you can add fields from the available fileds list in the left sidebar. We recommand fileds named "SessionID", "keywords”,“SessionURL" and "RequestURL".
![](https://raw.githubusercontent.com/quintinali/images/master/detailed%20log%20list.png)

Click view link in the table row, the system will take you to session tree structure page, where you could see the hierarchy treeview of the session, as well as all the requests in the session.

# Ontology Navigation
---


First,
## Vocabulary Relationship
---


First,
## Semantic Search
---


First,




#License
This source code is licensed under the [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0), a
copy of which is shipped with this project.

