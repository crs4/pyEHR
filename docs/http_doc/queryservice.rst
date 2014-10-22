Query Manager
=============

API Methods
-----------

.. http:get:: /check/status/querymanager

   Simple page to check if server is up and running.

.. http:post:: /check/status/querymanager

   Simple page to check if server is up and running.

.. http:post:: /query/execute

   Execute the given AQL query and apply the given (optional) parameters

   :query query: the AQL query that is going to be executed
   :query query_params: (optional) parameters that will be applied to the AQL query
   :resheader Content-Type: application/json
   :statuscode 200: query succesfully executed
   :statuscode 400: no `query` provided
   :statuscode 500: server error, error's details are specified in the returnded
                    response

The following query

.. code-block:: none

  SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
  o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS dyastolic
  FROM Ehr e
  CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]


will produce a response like

.. sourcecode:: json

 {
   "SUCCESS": true,
   "RESULTS_SET": {
     "results_count": 5,
     "results": [
       {"systolic": 120, "dyastolic": 115},
       {"systolic": 110, "dyastolic": 130},
       {"systolic": 140, "dyastolic": 90},
       {"systolic": 180, "dyastolic": 100},
       {"systolic": 220, "dyastolic": 160},
     ]
   }
 }

A query with parameters like the following

.. code-block:: none

  SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
  o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS dyastolic
  FROM Ehr e [uid=$ehrUid]
  CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]

will require a JSON like the following in the `query_params`

.. sourcecode:: json

  {
    "ehrUid": "PATIENT_00001"
  }