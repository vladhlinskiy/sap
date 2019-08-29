# SAP OData Batch Source

Description
-----------
This plugin reads data from SAP OData service.

Configuration
-------------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**OData Service URL:** Root URL of the SAP OData service.
The URL must end with an external service name (e.g., http://eccsvrname:8000/sap/opu/odata/sap/zgw100_dd02l_so_srv/).

**Resource Path:** Path of the SAP OData entity set. For example: "SalesOrderCollection". For more information,
see [OData URL components].

**Query:** OData query options to filter the data. For example: "$select=Name,Description&$top=10". For more
information, see [OData URL components].

[OData URL components]:
https://www.odata.org/documentation/odata-version-3-0/url-conventions/

**Username:** Username for basic authentication.

**Password:** Password for basic authentication.

**Output Schema:** Specifies the schema of the documents.


Data Types Mapping
----------

    | OData V2 Data Type              | CDAP Schema Data Type | Comment                                            |
    | ------------------------------- | --------------------- | -------------------------------------------------- |
    | Edm.Binary                      | bytes                 |                                                    |
    | Edm.Boolean                     | boolean               |                                                    |
    | Edm.Byte                        | int                   |                                                    |
    | Edm.DateTime                    | timestamp             |                                                    |
    | Edm.Decimal                     | decimal               |                                                    |
    | Edm.Double                      | double                |                                                    |
    | Edm.Single                      | float                 |                                                    |
    | Edm.Guid                        | string                |                                                    |
    | Edm.Int16                       | int                   |                                                    |
    | Edm.Int32                       | int                   |                                                    |
    | Edm.Int64                       | long                  |                                                    |
    | Edm.SByte                       | int                   |                                                    |
    | Edm.String                      | string                |                                                    |
    | Edm.Time                        | time                  |                                                    |
    | Edm.DateTimeOffset              | string                | Timestamp string in the following format:          |
    |                                 |                       | "2019-08-29T14:52:08.155+02:00"                    |
