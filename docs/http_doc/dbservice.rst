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

.. http:get:: /check/status/dbservice

   Simple page to check if server is up and running.

.. http:post:: /check/status/dbservice

   Simple page to check if server is up and running.

.. http:post:: /patient/add

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

.. http:post:: /patient/hide

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

.. http:post:: /patient/delete

   Delete patient with ID `patient_id`. If `cascade_delete` parameter is passed
   with a `True` vale, delete related EHRs as well, otherwise, if one or more EHRs
   are connected return an error.

   :query patient_id: ID of the patient
   :query cascade_delete: (optional) if True delete connected EHRs as well, if False
                          delete patient data only if no EHRs are connected, otherwise
                          return an error. Default value is `False`.
   :resheader Content-Type: application/json
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

.. http:post:: /patient/get

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
                        "archetype_class": "openEHR.TEST-EVALUATION.v1",
                        "archetype_details" {
                            "k2": "v2",
                            "k1": "v1"
                        }
                    },
                    "creation_time": 1399905956.765149,
                    "last_update": 1399905956.765149,
                    "record_id": "9a30f6b6a36b49c6b16e249ef35445eb",
                    "active": true,
                    "version": 1,
                },
                {
                    "ehr_data": {
                        "archetype_class": "openEHR.TEST-EVALUATION.v1",
                        "archetype_details": {
                            "k2": "v2",
                            "k1": "v1"
                        }
                    },
                    "creation_time": 1400244143.18824,
                    "last_update": 1400244143.18824,
                    "record_id": "e22332fcd4b7440585745bb2fe7866e5",
                    "active": true,
                    "version": 1,
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
                        "archetype_class": "openEHR.TEST-EVALUATION.v1",
                        "archetype_details": {}
                    },
                    "creation_time": 1399905956.765149,
                    "last_update": 1399905956.765149,
                    "record_id": "9a30f6b6a36b49c6b16e249ef35445eb",
                    "active": true,
                    "version": 1,
                },
                {
                    "ehr_data": {
                        "archetype_class": "openEHR.TEST-EVALUATION.v1",
                        "archetype_details": {}
                    },
                    "creation_time": 1400244143.18824,
                    "last_update": 1400244143.18824,
                    "record_id": "e22332fcd4b7440585745bb2fe7866e5",
                    "active": true,
                    "version": 1,
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

.. http:post:: /patient/load_ehr_records

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
                        "archetype_class": "openEHR.TEST-EVALUATION.v1",
                        "archetype_details": {}
                    },
                    "creation_time": 1399905956.765149,
                    "last_update": 1399905956.765149,
                    "record_id": "9a30f6b6a36b49c6b16e249ef35445eb",
                    "active": true,
                    "version": 1,
                },
                {
                    "ehr_data": {
                        "archetype_class": "openEHR.TEST-EVALUATION.v1",
                        "archetype_details": {}
                    },
                    "creation_time": 1400244143.18824,
                    "last_update": 1400244143.18824,
                    "record_id": "e22332fcd4b7440585745bb2fe7866e5",
                    "active": true,
                    "version": 1,
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
                        "archetype_class": "openEHR.TEST-EVALUATION.v1",
                        "archetype_details": {
                            "k2": "v2",
                            "k1": "v1"
                        }
                    },
                    "creation_time": 1399905956.765149,
                    "last_update": 1399905956.765149,
                    "record_id": "9a30f6b6a36b49c6b16e249ef35445eb",
                    "active": true,
                    "version": 1,
                },
                {
                    "ehr_data": {
                        "archetype_class": "openEHR.TEST-EVALUATION.v1",
                        "archetype_details": {
                            "k2": "v2",
                            "k1": "v1"
                        }
                    },
                    "creation_time": 1400244143.18824,
                    "last_update": 1400244143.18824,
                    "record_id": "e22332fcd4b7440585745bb2fe7866e5",
                    "active": true,
                    "version": 1,
                }
            ],
            "creation_time": 1399902042.211941,
            "last_update": 1400244143.221047
        },
        "SUCCESS": true
    }

.. http:post:: /ehr/add

   Add an EHR to an existing patient record.

   :query patient_id: ID of the patient record
   :query ehr_record: Archetype as a JSON dictionary, example provided below.
   :query record_id: (optional) the ID that will be given to this EHR record.
   :query creation_time: (optional) the creation timestamp for the record.
   :query active: (optional) True if the record must be saved as active (default),
                  False otherwise
   :resheader Content-Type: application/json
   :statuscode 200: record hidden succesfully or no patient with given ID
                    found (in this case a response with `SUCCESS` set to false
                    will be returned)
   :statuscode 400: no `patient_id` or `ehr_record` provided
   :statuscode 500: server error, error's details are specified in the
                    return response

   **Archetype JSON example**:

   Mandatory fields for an Archetype record are **archetype_class** and **archetype_details**.

   .. sourcecode:: json

      {
        "archetype_class": "openEHR.TEST-EVALUATION.v1",
        "archetype_details": {
          "at0001": "val1",
          "at0002": "val2"
        }
      }

   **Success response**:

   .. sourcecode:: json

      {
        "RECORD": {
          "ehr_data": {
            "archetype_class": "openEHR.TEST-EVALUATION.v1",
            "archetype_details": {
              "at0001": "val1",
              "at0002": "val2"
            }
          },
          "creation_time": 1399905956.765149,
          "last_update": 1399905956.765149,
          "record_id": "9a30f6b6a36b49c6b16e249ef35445eb",
          "active": true,
          "version": 1,
        },
        "SUCCESS": true
      }

.. http:post:: /ehr/hide

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

.. http:post:: /ehr/delete

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

.. http:post:: /ehr/get

   Get an EHR with a specific `ehr_record_id` related to a patient record with ID `patient_id`

   :query patient_id: ID of the patient record
   :query ehr_record_id: ID of the EHR
   :resheader Content-Type: application/json
   :statuscode 200: record successfully retrieved. If `patient_id` can't be mapped to a patient record
                    a response with `SUCCESS` set to False will be returned, the same thing happens
                    if the EHR with ID `ehr_record_id` is not connected to the patient record
   :statuscode 400: missing mandatory field `patient_id` or `ehr_record_id`
   :statuscode 500: generic server error, error's details are specified in the return response

   **Success response**

   .. sourcecode:: json

       {
         "record_id": "e22332fcd4b7440585745bb2fe7866e5",
         "patient_id": "JOHN_DOE",
         "version": 1,
         "active": true,
         "creation_time": 1416220308.61583,
         "last_update": 1416220308.61583,
         "ehr_data": {
            "archetype_class": "openEHR.TEST-EVALUATION.v1",
            "archetype_details": {
                "k1": "v1",
                "k2": "v2"
            }
         }
       }

.. http:post:: /batch/save/patient

   Save a patient and one or more related EHRs passed as a JSON document. If EHRs have a
   given ID and a duplicated key error is raisen, all EHRs of this batch previously saved
   and patient data will be automatically deleted. If a patient already exists, a
   duplicated key error will be raised and no data will be saved.

   :query patient_data: a JSON document with patient and EHRs data. Example provided below.
   :resheader Content-Type: application/json
   :statuscode 200: records succesfully saved
   :statuscode 400: missing mandatory field `patient_data`
   :statuscode 500: a patient with given ID already exists or one of the given EHRs has the
                    same ID of an existing record; data passed as `patient_data` can't be
                    mapped to pyEHR objects; generic server error, error's details are specified
                    in the return response

   **Patient Data JSON structure**

   .. sourcecode:: json

       {
         "record_id": "JOHN_DOE",
         "active": true,
         "ehr_records": [
           {
             "active": true,
             "version": 1,
             "creation_time": 1399905956.765149,
             "ehr_data" : {
               "archetype_class": "openEHR.TEST-EVALUATION.v1",
               "archetype_details": {
                 "at0001": "val1",
                 "at0002": "val2"
               }
             }
           },
           {
             "active": true,
             "version": 1,
             "creation_time": 1399905956.895149,
             "ehr_data": {
               "archetype_class": "openEHR.TEST-EVALUATION-BIS.v1",
               "archetype_details": {
                 "at0001": "val1",
                 "at0002": "val2",
                 "at0003": {
                   "archetype_class": "openEHR.TEST-EVALUATION-BIS_SUBMODULE.v1",
                   "archetype_details": {
                     "at0001": "val1",
                     "at0002": "val2"
                   }
                 }
               }
             }
           }
         ]
       }

.. http:post:: /batch/save/patients

   Save a list of patients with related EHRs passed as a JSON document. For each patient
   in the set, if EHRs have a given ID and a duplicated key error is raisen, all EHRs of
   the batch previously saved and patient data will be automatically deleted. If one of
   the patients already exists, a duplicated key error will be raised and that patient's
   batch won't be saved.

   :query patients_data: a JSON document with patients and EHRs data. For each patient, the
                         method will accept the same structure of the `batch/save/patient/`
                         method, patients must be enclosed within a list.
   :resheader Content-Type: application/json
   :statuscode 200: operation completed, saved records and the one that raised and error will
                    be specified in the return response
   :statuscode 400: missing mandatory field `patients_data`

   **Success response**

   .. sourcecode:: json

    {
      "SUCCESS": true,
      "SAVED": [
        {
          "record_id": "GOOD_PATIENT",
          "active": true,
          "version": 1,
          "creation_time": 1399905956.765149,
          "last_update": 1399905956.765149,
          "ehr_records": [
            {
              "active": true,
              "creation_time": 1399905956.765149,
              "last_update": 1399905956.765149,
              "ehr_data": {
                "archetype_class": "openEHR.TEST-EVALUATION.v1",
                "archetype_details": {
                  "at0001": "val1",
                  "at0002": "val2"
                }
              }
            }
          ]
        }
      ],
      "ERRORS": [
        {
          "MESSAGE": "Duplicated key error for patient with ID BAD_PATIENT",
          "RECORD": {
            "record_id": "BAD_PATIENT",
            "ehr_records": [
              {
                "ehr_data": {
                  "archetype_class": "openEHR.TEST-EVALUATION.v1",
                  "archetype_details": {
                    "at0001": "val1",
                    "at0002": "val2"
                  }
                }
              }
            ]
          }
        }
      ]
    }