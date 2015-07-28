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

.. http:put:: /patient

   Add a new patient with ID `patient_id`

   :query patient_id: ID of the new patient as string
   :query creation_time: (optional) creation time of the record as a
                         float timestamp. If no value is provided,
                         record's creation time will be assigned when
                         the Python object is created
   :query active: (optional) a boolean value encoded as string,
                  indicates if the record will be an active one or
                  not. Default value is 'True'.
   :reqheader Content-Type: application/json
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

.. http:delete:: /patient/(patient_id)/hide

   Hide patient with ID (`patient_id`). Related EHRs will be hidden as well.

   :reqheader Content-Type: application/json
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

.. http:delete:: /patient/(patient_id)/delete

   Delete patient with ID (`patient_id`). If `cascade_delete` parameter is passed
   with a `True` value, delete related EHRs as well, otherwise, if one or more EHRs
   are connected return an error.

   :query cascade_delete: (optional) if True delete connected EHRs as well, if False
                          delete patient data only if no EHRs are connected, otherwise
                          return an error. Default value is `False`.
   :reqheader Content-Type: application/json
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

.. http:get:: /patient/(patient_id)

   Get patient with ID (`patient_id`) and related EHRs. If `fetch_ehr_records` parameter
   is passed with a `False` value, only fetch a minimum amount of details for the EHRs
   (ID, archetype but no clinical data details). If `fetch_hidden_ehr_records` parameter
   is passed with a `True` value fetch also hidden EHRs.

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

.. http:get:: /patient/load_ehr_records

   Load EHR records data for a given patient record (in JSON format), this method is intended
   to be used if the patient record was retrieved with the `fetch_ehr_records` flag
   set up to False. Only the EHRs (in JSON format) embedded in the patient record will
   be loaded, other records connected to the given patient record will be ignored.

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

   :reqheader Content-Type: application/json
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

.. http:put:: /ehr

   Add an EHR to an existing patient record.

   :query patient_id: ID of the patient record
   :query ehr_record: Archetype as a JSON dictionary, example provided below.
   :query record_id: (optional) the ID that will be given to this EHR record.
   :query creation_time: (optional) the creation timestamp for the record.
   :query active: (optional) True if the record must be saved as active (default),
                  False otherwise
   :reqheader Content-type: application/json
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

.. http:delete:: /ehr/(patient_id)/(ehr_record_id)/hide

   Hide an EHR with a specific (`ehr_record_id`) related to a patient record with ID (`patient_id`)

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

.. http:delete:: /ehr/(patient_id)/(ehr_record_id)/delete

   Delete an EHR with a specific (`ehr_record_id`) related to a patient record with ID (`patient_id`)

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

.. http:get:: /ehr/(patient_id)/(ehr_record_id)

   Get an EHR with a specific (`ehr_record_id`) related to a patient record with ID (`patient_id`)

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

.. http:post:: /ehr/update

   Use the given EHR record to update an existing one stored in the database. Provided record must
   be a valid and complete one (like the ones returned by a GET request) with valid ID (used to lookup
   the record that will be replaced) and a VERSION that must be greater than or equal to 1 (otherwise
   the record will be considered as "unmapped" and won't be used).
   If given record is identical to the one that will be replaced, no update operation will be performed
   (avoiding useless updates).
   If an older version of the EHR record is used to update the existing one (*version* and *last_update*
   fields are lower than the ones of the record in the DB), and Optimistic Lock Error will be returned
   and no update will be performed.
   If the update is performed correctly, the new version of the EHR document will be returned with
   the response as a JSON document.

   :query ehr_record: a JSON document with the EHR record that will be used to update an existing
                      one (record's ID will be used to choose which record will be updated).
   :reqheader Content-Type: application/json
   :resheader Content-Type: application/json
   :statuscode 200: record sucessfully updated. If a Redundant Update Error or an Optimistick Lock
                    Error is raised, return a JSON document with a `SUCCESS` field set to False
                    and a `MESSAGE` field used to describe the error.
   :statuscode 400: missing mandatory field `ehr_record`
   :statuscode 500: EHR record's structure can't be mapped properly to a ClinicalRecord object; a non
                    persistent EHR record is used to update an existing one; generic server error

   **Update example**

   The following record is mapped in the DB and is the one that will be updated

   .. sourcecode:: json

      {
        "record_id": "74d740cb2c914f32adc3dbc371ceb8a8",
        "patient_id": "TEST_PATIENT",
        "creation_time": 1438076222.165714,
        "last_update": 1438076222.165714,
        "version": 1,
        "active": true,
        "ehr_data": {
           "archetype_class": "openEHR-EHR-OBSERVATION.test-observation.v1",
           "archetype_details": {
              "at0001": "value1",
              "at0002": "value2"
           }
        }
      }

   The updated record that will be passed to the HTTP server will be

   .. sourcecode:: json

      {
        "record_id": "74d740cb2c914f32adc3dbc371ceb8a8",
        "patient_id": "TEST_PATIENT",
        "creation_time": 1438076222.165714,
        "last_update": 1438076222.165714,
        "version": 1,
        "active": true,
        "ehr_data": {
           "archetype_class": "openEHR-EHR-OBSERVATION.test-observation.v1",
           "archetype_details": {
              "at0001": "new_value1",
              "at0002": "value2",
              "at0003": {
                 "archetype_class": "openEHR-EHR-OBSERVATION.test-inner-observation.v1",
                 "ehr_data": {
                    "at100.1": "value1.1",
                    "at100.2": "value1.2"
                 }
              }
           }
        }
      }

   *Note well*  that `version`, `record_id` and `last_update` field should be left untouched

   The response from the HTTP server will be

   .. sourcecode:: json

      {
        "SUCCESS": true,
        "RECORD": {
          "record_id": "74d740cb2c914f32adc3dbc371ceb8a8",
          "patient_id": "TEST_PATIENT",
          "creation_time": 1438076222.165714,
          "last_update": 1442144124.141244,
          "version": 2,
          "active": true,
          "ehr_data": {
            "archetype_class": "openEHR-EHR-OBSERVATION.test-observation.v1",
            "archetype_details": {
              "at0001": "new_value1",
              "at0002": "value2",
              "at0003": {
                "archetype_class": "openEHR-EHR-OBSERVATION.test-inner-observation.v1",
                "ehr_data": {
                  "at100.1": "value1.1",
                  "at100.2": "value1.2"
                }
              }
            }
          }
        }
      }

.. http:put:: /batch/save/patient

   Save a patient and one or more related EHRs passed as a JSON document. If EHRs have a
   given ID and a duplicated key error is raisen, all EHRs of this batch previously saved
   and patient data will be automatically deleted. If a patient already exists, a
   duplicated key error will be raised and no data will be saved.

   :query patient_data: a JSON document with patient and EHRs data. Example provided below.
   :reqheader Content-Type: application/json
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

.. http:put:: /batch/save/patients

   Save a list of patients with related EHRs passed as a JSON document. For each patient
   in the set, if EHRs have a given ID and a duplicated key error is raisen, all EHRs of
   the batch previously saved and patient data will be automatically deleted. If one of
   the patients already exists, a duplicated key error will be raised and that patient's
   batch won't be saved.

   :query patients_data: a JSON document with patients and EHRs data. For each patient, the
                         method will accept the same structure of the `batch/save/patient/`
                         method, patients must be enclosed within a list.
   :reqheader Content-Type: application/json
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
          "creation_time": 1399905956.765149,
          "last_update": 1399905956.765149,
          "ehr_records": [
            {
              "record_id": "74d740cb2c914f32adc3dbc371ceb8a8",
              "active": true,
              "creation_time": 1399905956.765149,
              "last_update": 1399905956.765149,
              "version": 1,
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