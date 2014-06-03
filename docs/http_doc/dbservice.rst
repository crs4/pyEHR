DB Service
==========

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

   Delete patient with ID `patient_id`. If `cascade_delete` parameter is passed
   with a `True` vale, delete related EHRs as well, otherwise, if one or more EHRs
   are connected return an error.

   :query patient_id: ID of the patient
   :query cascade_delete: (optional) if True delete connected EHRs as well, if False
                          delete patient data only if no EHRs are connected, otherwise
                          return an error. Default value is `False`.
   :resheader Conent-Type: application/json
   :statuscode 200: record deleted succesfully or no patient with given ID found (in this
                    case a response with `SUCCESS` set to false will be returned)
   :statuscode 400: no `patient_id` provided
   :statuscode 500: patient has connected EHRs and `cascade_delete` is set to false or
                    server error, error's details are specified in the return response

   **Success response**

   .. sourcecode:: json

      {
        "SUCCESS": true,
        "MESSAGE": "Patient record with ID FOOBAR successfully deleted"
      }

.. http:post:: /patient/get/

   Get patient with ID `patient_id` and related EHRs. If `fetch_ehr_records` parameter
   is passed with a `False` value, only fetch a minimum amount of details for the EHRs
   (ID, archetype but no clinical data details). If `fetch_hidden_ehr_records` parameter
   is passed with a `True` value fetch also hidden EHRs.

   :query patient_id: ID of the patient
   :query fetch_ehr_records: (optional) if False only get a minimal version of the connected
                             EHRs (ID, archetype but no clinical data details). Default value
                             is `True`
   :query fetch_hidden_ehr_records: (optional) if True, fetch hidden EHRs (the one with `active`
                                    value set to `False`). Default value is `False`
   :resheader Content-Type: application/json
   :statuscode 200: data retrieved succesfully or no patient with given ID found, in this
                    case `RECORD` field of the response will be `NULL`
   :statuscode 400: no `patient_id` provided
   :statuscode 500: server error, error's details are specified in the return response

   **Success response**

   With `fetch_ehr_records` set to `True`

   .. sourcecode:: json

    {
        "RECORD": {
            "record_id": "FOOBAR",
            "active": true,
            "ehr_records": [
                {
                    "ehr_data": {
                        "archetype": "openEHR.TEST-EVALUATION.v1",
                        "data" {
                            "k2": "v2",
                            "k1": "v1"
                        }
                    },
                    "creation_time": 1399905956.765149,
                    "last_update": 1399905956.765149,
                    "record_id": "9a30f6b6a36b49c6b16e249ef35445eb",
                    "active": true,
                },
                {
                    "ehr_data": {
                        "archetype": "openEHR.TEST-EVALUATION.v1",
                        "data": {
                            "k2": "v2",
                            "k1": "v1"
                        }
                    },
                    "creation_time": 1400244143.18824,
                    "last_update": 1400244143.18824,
                    "record_id": "e22332fcd4b7440585745bb2fe7866e5",
                    "active": true,
                }
            ],
            "creation_time": 1399902042.211941,
            "last_update": 1400244143.221047
        },
        "SUCCESS": true
    }

   With `fetch_ehr_records` set to `False`

   .. sourcecode:: json

    {
        "RECORD": {
            "record_id": "FOOBAR",
            "active": true,
            "ehr_records": [
                {
                    "ehr_data": {
                        "archetype": "openEHR.TEST-EVALUATION.v1",
                        "data": {}
                    },
                    "creation_time": 1399905956.765149,
                    "last_update": 1399905956.765149,
                    "record_id": "9a30f6b6a36b49c6b16e249ef35445eb",
                    "active": true,
                },
                {
                    "ehr_data": {
                        "archetype": "openEHR.TEST-EVALUATION.v1",
                        "data": {}
                    },
                    "creation_time": 1400244143.18824,
                    "last_update": 1400244143.18824,
                    "record_id": "e22332fcd4b7440585745bb2fe7866e5",
                    "active": true,
                }
            ],
            "creation_time": 1399902042.211941,
            "last_update": 1400244143.221047
        },
        "SUCCESS": true
    }

   If `patient_id` doesn't match any patient record

   .. sourcecode:: json

    {
        "SUCCESS": true,
        "RECORD": null
    }

.. http:post:: /patient/load_ehr_records/

   Load EHR records data for a given patient record (in JSON format), this method is usefull
   if the patient record was retrieved with the `fetch_ehr_records` flag set up to False.
   Only the EHRs (in JSON format) embedded in the patient record will be loaded,
   other records connected to the given patient record will be ignored.

   :query patient_record: a patient record in JSON format with unloaded EHRs (clinical records
                          withouth clinical data details). An example is the following

   .. sourcecode:: json

    {
        "RECORD": {
            "record_id": "FOOBAR",
            "active": true,
            "ehr_records": [
                {
                    "ehr_data": {
                        "archetype": "openEHR.TEST-EVALUATION.v1",
                        "data": {}
                    },
                    "creation_time": 1399905956.765149,
                    "last_update": 1399905956.765149,
                    "record_id": "9a30f6b6a36b49c6b16e249ef35445eb",
                    "active": true,
                },
                {
                    "ehr_data": {
                        "archetype": "openEHR.TEST-EVALUATION.v1",
                        "data": {}
                    },
                    "creation_time": 1400244143.18824,
                    "last_update": 1400244143.18824,
                    "record_id": "e22332fcd4b7440585745bb2fe7866e5",
                    "active": true,
                }
            ],
            "creation_time": 1399902042.211941,
            "last_update": 1400244143.221047
        },
        "SUCCESS": true
    }

   :resheader Content-Type: application/json
   :statuscode 200: operation succesfully completed
   :statuscode 400: no `patient_record` provided
   :statuscode 500: invalid JSON format for the provided `patient_record` or generic
                    server error, error's details are specified in the return response

   **Success response**

   .. sourcecode:: json

    {
        "RECORD": {
            "record_id": "FOOBAR",
            "active": true,
            "ehr_records": [
                {
                    "ehr_data": {
                        "archetype": "openEHR.TEST-EVALUATION.v1",
                        "data": {
                            "k2": "v2",
                            "k1": "v1"
                        }
                    },
                    "creation_time": 1399905956.765149,
                    "last_update": 1399905956.765149,
                    "record_id": "9a30f6b6a36b49c6b16e249ef35445eb",
                    "active": true,
                },
                {
                    "ehr_data": {
                        "archetype": "openEHR.TEST-EVALUATION.v1",
                        "data": {
                            "k2": "v2",
                            "k1": "v1"
                        }
                    },
                    "creation_time": 1400244143.18824,
                    "last_update": 1400244143.18824,
                    "record_id": "e22332fcd4b7440585745bb2fe7866e5",
                    "active": true,
                }
            ],
            "creation_time": 1399902042.211941,
            "last_update": 1400244143.221047
        },
        "SUCCESS": true
    }

.. http:post:: /ehr/add/

   Add an EHR to an existing patient record.

   :query patient_id: ID of the patient record
   :query ehr_record: EHR as a JSON dictionary. EHR example provided below.
   :resheader Content-Type: application/json
   :statuscode 200: record hidden succesfully or no patient with given ID
                    found (in this case a response with `SUCCESS` set to false
                    will be returned)
   :statuscode 400: no `patient_id` or `ehr_record` provided
   :statuscode 500: server error, error's details are specified in the
                    return response

   **EHR JSON example**:

   Mandatory fields for an EHR are **archetype** and **ehr_data**.

   .. sourcecode:: json

      {
        "ehr_data": {
          "archetype": "openEHR.TEST-EVALUATION.v1",
          "data": {
            "at0001": "val1",
            "at0002": "val2"
          }
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
            "archetype": "openEHR.TEST-EVALUATION.v1",
            "data": {
              "at0001": "val1",
              "at0002": "val2"
            }
          },
          "creation_time": 1399905956.765149,
          "last_update": 1399905956.765149,
          "record_id": "9a30f6b6a36b49c6b16e249ef35445eb",
          "active": true,
        },
        "SUCCESS": true
      }

.. http:post:: /ehr/hide/

   Hide an EHR with a specific `ehr_record_id` related to a patient record with ID `patient_id`

   :query patient_id: ID of the patient record
   :query ehr_record_id: ID of the EHR
   :resheader Content-Type: application/json
   :statuscode 200: record succesfully hidden. If `patient_id` can't be mapped to a patient record
                    a response with `SUCCESS` set to False will be returned, the same thing happens
                    if the EHR with ID `ehr_record_id` is not connected to the patient record or if
                    it is already an hidden record.
   :statuscode 400: missing mandatory field `patient_id` or `ehr_record_id`
   :statuscode 500: generic server error, error's details are specified in the return response

   **Success response**

   If `patient_id` can't be mapped to a patient within the database

   .. sourcecode:: json

    {
      "SUCCESS": false,
      "MESSAGE": "There is no patient record with ID JOHN_DOE"
    }

   If `ehr_record_id` is not an EHR connected to the specified patient record

   .. sourcecode:: json

    {
      "SUCCESS": false,
      "MESSAGE": "EHR record with ID 123456 is not connected to patient record or is already an hidden record"
    }

   If record is successfully hidden

   .. sourcecode:: json

    {
      "SUCCESS": true,
      "MESSAGE": "EHR record with ID 9a30f6b6a36b49c6b16e249ef35445eb succesfully hidden"
    }

.. http:post:: /ehr/delete/

   Delete an EHR with a specific `ehr_record_id` related to a patient record with ID `patient_id`

   :query patient_id: ID of the patient record
   :query ehr_record_id: ID of the EHR
   :resheader Content-Type: application/json
   :statuscode 200: record succesfully deleted. If `patient_id` can't be mapped to a patient record
                    a response with `SUCCESS` set to False will be returned, the same thing happens
                    if the EHR with ID `ehr_record_id` is not connected to the patient record
   :statuscode 400: missing mandatory field `patient_id` or `ehr_record_id`
   :statuscode 500: generic server error, error's details are specified in the return response

   **Success response**

   If `patient_id` can't be mapped to a patient within the database

   .. sourcecode:: json

    {
      "SUCCESS": false,
      "MESSAGE": "There is no patient record with ID JOHN_DOE"
    }

   If `ehr_record_id` is not an EHR connected to the specified patient record

   .. sourcecode:: json

    {
      "SUCCESS": false,
      "MESSAGE": "Patient record FOOBAR is not connected to an EHR record with ID 123456"
    }

   If record is successfully hidden

   .. sourcecode:: json

    {
      "SUCCESS": true,
      "MESSAGE": "EHR record with ID 9a30f6b6a36b49c6b16e249ef35445eb succesfully deleted"
    }

.. http:post:: /batch/save/patient/

   TBD

.. http:post:: /batch/save/patients/

   TBD