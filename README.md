# Property Graph Manager

This repository contains source code of a small Java application, which can be used to synchronize published property graphs with tables used in their definitions.
This application supports synchronization of multiple graphs loaded and published in a single one PGX server instance; every graph is maintained in a separate thread.  
The application can work in one of two modes:
* <B>SINGLE_RUN</B> In this mode the application loads graphs (if needed), synchronizes them and quits
* <B>BACKGROUND_MODE</B> In this mode the application loads graphs (if needed) and enters the loop in which it checks the existence of the following label files in PGX_LABEL_DIR directory:
* <B>GraphName.synclabel</B> - if this file is created, then the next synchronization for the graph maintained in this thread starts
* <B>GraphName.stoplabel</B> - if this file is created, then the thread maintaining the graph with <GraphName> name releases all resources, stops synchronizing the graph and quits.
* <B>pgm.stoplabel</B> - if this file is created, then all threads stop synchronizing their graphs, release all resources and quits.
* <B>pgm.showlabel</B> - if this file is created, then PGM displays basic system information.

Requirements:
* JDK18
* Access to Graph Server and Oracle Database instances as property graph owner
* All requirements for synchronizeable published graphs needs to be met (see Oracle Graph Server documentation)
* The following environment variables need to be set
  * PGX_EXECUTION_MODE   information about the mode of execution of the application. It can  be set to SINGLE_RUN or BACKGROUND values (see the description of modes)
  * PGX_RELOAD           information whether to restart the PGX server instance or not (true|false)
  * PGX_LABEL_DIR        directory for label files. Creation of this file causes next resynchronization. At the end of synchronization this file is automatically deleted.
  * PGX_CONFIG_DIR       directory where JSON definition of graphs are stored by the application. This directory must exist and is maintained automatically by the application. We should NOT change the content of this directory or any of files stored in it.
  * PGX_URL              PGX Server URL
  * PGX_JDBC_URL         Oracle db JDBC URL
  * PGX_USERNAME         Username
  * PGX_PASSWORD         Password
  * PGX_GRAPH_NAMES      List of names of graps we want to synchronize
 
# License

Copyright (c) 2024 Oracle and/or its affiliates.

Licensed under the Universal Permissive License (UPL), Version 1.0.

See [LICENSE](https://github.com/oracle-devrel/technology-engineering/blob/main/LICENSE) for more details.
