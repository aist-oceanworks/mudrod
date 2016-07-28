# MUDROD GUI Guide

Background Information
---
This tutorial is designed to enable the user to successfully use web service component of MUDROD.

The MUDROD GUI enables the user to:

1. Explore imported logs
2. Learn session structure and details
3. Interact with the vocabulary ontology graph
4. Find datasets using semantic search
    

This tutorial will require MUDROD to already be published as a web service. Please refer to [**Installing MUDROD on CentOS**](https://github.com/mudrod/mudrod/blob/master/Installation.md)

Mudrod provides two kinds of web services. One web service is based on Kibana - a visulization tool of ELK stack. The port of this service is 5601. Another one is a java web service developed on our own, which is published through tomcat, with a port of 8080.

# Log Exploration:
---


First, visit the homepage of kibana:

    http://your_public_ip:5601/

> The first time you visit this page, the system will let you configure an index pattern. Please input the index name in the "index name or pattern" field, such as mudrod, and then choose the Time-field name.
![](https://raw.githubusercontent.com/quintinali/images/master/add%20index.png)
> After clicking the create button, all fields in the index will be listed. Please find a filed named "SessionURL" and edit it the following ways:
1. Change format to URL
2. Input "view" in label template and save it.
![](https://raw.githubusercontent.com/quintinali/images/master/edit%20index.png)

Next, choose the "Discover" tab at the menu bar. 

![](https://raw.githubusercontent.com/quintinali/images/master/discover%20default.png)

If the system tells you that no result is found, please change the time range by clicking the top right area. The following graph shows how to set the time range from 2014-01-01 to 2014-12-31 using absolute value.

![](https://raw.githubusercontent.com/quintinali/images/master/change%20time%20range.png)

Once the time range is set, logs falling into the range are shown in the main area of the discover page.

![](https://raw.githubusercontent.com/quintinali/images/master/defaul%20log%20list.png)

If you are interested in data visulaization and want to explore the log data deeply, please refer to [**Kibana User Guide**](https://www.elastic.co/guide/en/kibana/4.1/index.html)


# Session Structure
---


First, visit the homepage of kibana as we described in Log Exploration section.

Next, input "sessionstats*" in the search box. The table below the histogram will list data in types whose names contain "sessionstats".
![](https://raw.githubusercontent.com/quintinali/images/master/improved%20log%20list.png)

In order to get a brief overview of sessions, you can add fields from the available fields list in the left sidebar. We recommend the fields named "SessionID", "keywords”,“SessionURL" and "RequestURL".
![](https://raw.githubusercontent.com/quintinali/images/master/detailed%20log%20list.png)

Click view link in the table row. The system will take you to the session tree structure page, where you can see the hierarchy treeview of the session as well as all of the requests in the session.
![](https://raw.githubusercontent.com/quintinali/images/master/session%20tree.png)

# Ontology Navigation
---


First, visit the homepage of MUDROD web service:

    http://your_public_ip:8080/mudrod-service/
## Vocabulary Relationship
---
Select "Vocabulary linkage" in the left side bar and input words in the search box.

![](https://raw.githubusercontent.com/quintinali/images/master/vacobulary%20linkage.png)

The ontology search results contain the most related words to the user queries with their associated linkage weight. If you double click any node in the graph, the system will show words related to the vocabulary word you just selected.
![](https://raw.githubusercontent.com/quintinali/images/master/vl%20result.png)

## Semantic Search
---
Select "Semantic search" in the left side bar.
![](https://raw.githubusercontent.com/quintinali/images/master/search.png)

Next, search datasets by keywords. Related datasets are listed in a descending order of relevance.

![](https://raw.githubusercontent.com/quintinali/images/master/search%20result.png)




#License
This source code is licensed under the [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0), a
copy of which is shipped with this project.

