# MarkLogic Batch Sink


Description
-----------
Writes records to MarkLogic. Each record will be written to the separate file.


Configuration
-------------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Host:** Host that MarkLogic is running on.

**Port:** Port that MarkLogic is listening to.

**Database:** MarkLogic database name (May be no set if database is bounded to dedicated app server).

**Path:** Path to write documents to.

**File Name Field:** Which input field will be used to generate file name. If this field is not set, than UUID will be generated.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Type:** The type of connection to use - Direct or Gateway to
[connect through a load balancer](https://docs.marklogic.com/guide/java/intro#id_78775).

**Authentication Type:** The type of authentication to use - Digest or SSL.

**Format:**	Type of document (JSON/XML/DELIMITED), default: JSON.

**Delimiter:**	Delimiter if the format is 'delimited'.

**Batch Size:**	The batch size for writing to MarkLogic.