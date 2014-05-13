Common Responses
----------------
Error reponses are the same for all the methods exposed by the REST API,
response has a `SUCCESS` field with value false and an `ERROR` field with a short
description of the occurred error. An example is the following:

.. sourcecode:: json

   {
     "SUCCESS": false,
     "ERROR": "Missing mandatory field, can't continue with the request"
   }

API Methods
-----------

.. http:get:: /check/status/

   Simple page to check if server is up and running.

.. http:post:: /check/status/

   Simple page to check if server is up and running.

.. http:post:: /patient/add/

   Add a new patient with ID `patient_id`

   :query patient_id: ID of the new patient as string
   :query creation_time: (optional) creation time of the record as a
                         float timestamp. If no value is provided,
                         record's creation time will be assigned when
                         the Python object is created
   :query active: (optional) a boolean value encoded as string,
                  indicates if the record will be an active one or
                  not. Default value is 'True'.
   :resheader Content-Type: application/json
   :statuscode 200: record saved succesfully
   :statuscode 400: no `patient_id` provided
   :statuscode 500: server error, error's details are specified in the
                    returned response

   **Success response**:

   .. sourcecode:: json
   
      {
        "RECORD": {
          "record_id": "FOOBAR",
          "active": true,
          "ehr_records": [],
          "creation_time": 1399902042.211941,
          "last_update": 1399902042.211941
        },
        "SUCCESS": true
      }

.. http:post:: /patient/hide/

   Hide patient with ID `patient_id`. Related EHRs will be hidden as well.

   :query patient_id: ID of the patient
   :resheader Content-Type: application/json
   :statuscode 200: record hidden succesfully or no patient with given ID
                    found (in this case a response with `SUCCESS` set to false
                    will be returned)
   :statuscode 400: no `patient_id` provided
   :statuscode 500: server error, error's detail are specified in the
                    return response

   **Success response**:

   .. sourcecode:: json

      {
        "RECORD": {
          "record_id": "FOOBAR",
          "active": false,
          "ehr_records": [],
          "creation_time": 1399902042.211941,
          "last_update": 1399902042.211941
        },
        "SUCCESS": true
      }

.. http:post:: /patient/delete/

   TBD

.. http:post:: /patient/get/

   TBD

.. http:post:: /patient/load_ehr_records/

   TBD

.. http:post:: /ehr/add/

   Add an EHR to an existing patient record.

   :query patient_id: ID of the patient record
   :query ehr_record: EHR as a JSON dictionary. EHR example provided below.
   :resheader Content-Type: application/json
   :statuscode 200: record hidden succesfully or no patient with given ID
                    found (in this case a response with `SUCCESS` set to false
                    will be returned)
   :statuscode 400: no `patient_id` or `ehr_record` provided
   :statuscode 500: server error, error's detail are specified in the
                    return response

   **EHR JSON example**:

   Mandatory fields for an EHR are **archetype** and **ehr_data**.

   .. sourcecode:: json

      {
        "archetype": "openEHR.TEST-EVALUATION.v1",
        "ehr_data": {
          "at0001": "val1",
          "at0002": "val2"
        },
        "active": true,
        "creation_time": 1399902042.311941,
        "record_id": "c1a5b6e68bb34b6baca21c683037e255"
      }

   **Success response**:

   .. sourcecode:: json

      {
        "RECORD": {
          "ehr_data": {
            "at0001": "val1",
            "at0002": "val2"
          },
          "creation_time": 1399905956.765149,
          "last_update": 1399905956.765149,
          "record_id": "9a30f6b6a36b49c6b16e249ef35445eb",
          "active": true,
          "archetype": "openEHR.TEST-EVALUATION.v1"
        },
        "SUCCESS": true
      }

.. http:post:: /ehr/hide/

   TBD

.. http:post:: /ehr/delete/

   TBD

.. http:post:: /batch/save/patient/

   TBD

.. http:post:: /batch/save/patients/

   TBD