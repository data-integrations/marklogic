# MarkLogic Action


Description
-----------
Action that runs a MarkLogic command.


Configuration
-------------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Host:** Host that MarkLogic is running on.

**Port:** Port that MarkLogic is listening to.

**Database:** MarkLogic database name (May be no set if database is bounded to dedicated app server).

**Query:** XQuery for MarkLogic, can be not set.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Type:** The type of connection to use - Direct or Gateway to
[connect through a load balancer](https://docs.marklogic.com/guide/java/intro#id_78775).

**Authentication Type:** The type of authentication to use - Digest or SSL.