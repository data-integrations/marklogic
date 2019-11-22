# MarkLogic Batch Source


Description
-----------
Reads documents from a MarkLogic and converts each document into a StructuredRecord with the help
of a specified schema. The user can optionally provide input query.


Configuration
-------------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Host:** Host that MarkLogic is running on.

**Port:** Port that MarkLogic is listening to.

**Database:** MarkLogic database name (May be no set if database is bounded to dedicated app server).

**Input Method:** Method to get files: QUERY or PATH.

**Path:** Path to read documents from.

**Input Query:** XQuery for MarkLogic, read all documents, if not set.

**Output Schema:** Specifies the schema of the documents.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Type:** The type of connection to use - Direct or Gateway to
[connect through a load balancer](https://docs.marklogic.com/guide/java/intro#id_78775).

**Authentication Type:** The type of authentication to use - Digest or SSL.

**Bounding Query:** Bounding Query should return (forest_id, record_count, host_name).

**Max Splits:** Maximum amount of splits.

**Format:**	Type of document (AUTO/JSON/XML/TEXT/BLOB/DELIMITED), default: AUTO	

**Delimiter:**	Delimiter if the format is 'delimited'

**File Name Field:** Field to store information about the file

**Payload Field:**	Field to store data from Binary and Text files