# Property Graph Manager

This repository contains source code of a small Java application, which can be used to synchronize published property graphs with tables used in their definitions.
This application supports synchronization of multiple graphs loaded and published in a single one PGX server instance; every graph is maintained in a separate thread. Threads check for existence of four label files in a directory pointed by PGX_LABEL_DIR environment variable:
* <B>GraphName.synclabel</B> - if this file is created, then the next synchronization for the graph maintained in this thread starts
* <B>GraphName.stoplabel</B> - if this file is created, then the thread maintaining the graph with <GraphName> name releases all resources, stops synchronizing the graph and quits.
* <B>pgm.stoplabel</B> - if this file is created, then all threads stop synchronizing their graphs, release all resources and quits.
* <B>pgm.showlabel</B> - if this file is created, then PGM displays basic system information.

Requirements:
* JDK18
* Access to Graph Server and Oracle Database instances as property graph owner
* All requirements for synchronizeable published graphs needs to be met (see Oracle Graph Server documentation)
* The following environment variables need to be set
  * PGX_RELOAD           information whether to restart the PGX server instance or not (true|false)
  * PGX_LABEL_DIR        directory for label files. Creation of this file causes next resynchronization. At the end of synchronization this file is automatically deleted.
  * PGX_URL              PGX Server URL
  * PGX_JDBC_URL         Oracle db JDBC URL
  * PGX_USERNAME         Username
  * PGX_PASSWORD         Password
  * PGX_GRAPH_NAMES      List of names of graps we want to synchronize
 
# License

Copyright (c) 2024 Oracle and/or its affiliates.

Licensed under the Universal Permissive License (UPL), Version 1.0.

See [LICENSE](https://github.com/oracle-devrel/technology-engineering/blob/main/LICENSE) for more details.
