# Property Graph Server Graph Manager

This repository contains source code of a small Java application, which can be used to synchronize the published property graph with the tables used in its definition.
Requirements:
* JDK18
* Access to Graph Server and Oracle Database instances as property graph owner
* The following environment variables need to be set
  * PGX_RELOAD           information whether to restart the PGX server instance or not (true|false)
  * PGX_APP_EXEC_MODE    NOHUP or INTERACTIVE execution modes
  * PGX_LABEL_FILE_NAME  Label file name for NOHUP mode. Creation of this file causes next resynchronization. At the end of synchronization this file is automatically deleted.
  * PGX_URL              PGX Server URL
  * PGX_JDBC_URL         Oracle db JDBC URL
  * PGX_USERNAME         Username
  * PGX_PASSWORD         Password
  * PGX_GRAPH_NAME       Name of the graph, which need to be synchronized
# License

Copyright (c) 2024 Oracle and/or its affiliates.

Licensed under the Universal Permissive License (UPL), Version 1.0.

See [LICENSE](https://github.com/oracle-devrel/technology-engineering/blob/main/LICENSE) for more details.
