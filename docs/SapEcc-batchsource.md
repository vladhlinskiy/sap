# SAP ECC Batch Source

Description
-----------
This plugin reads data from SAP ECC OData service.

Configuration
-------------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**OData Service URL:** Root URL of the SAP ECC OData service.
The URL must end with an external service name (e.g., http://eccsvrname:8000/sap/opu/odata/sap/zgw100_dd02l_so_srv/).

**Resource Path:** Path of the SAP ECC OData entity. For example: "Category(1)/Products". For more information,
see [OData URL components].

**Query:** OData query options to filter the data. For example: "$select=Name,Description&$top=10". For more
information, see [OData URL components].

[OData URL components]:
https://www.odata.org/documentation/odata-version-3-0/url-conventions/

**Username:** Username for basic authentication.

**Password:** Password for basic authentication.

**Output Schema:** Specifies the schema of the documents.


# TODO mappings