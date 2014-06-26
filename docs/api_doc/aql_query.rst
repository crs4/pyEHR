AQL queries execution
=====================

Resolve the CONTAIN statement
-----------------------------

The CONTAINS statement of an AQL query can be resolved using the pyEHR's Index Service.
The Index Service is responsible to store an XML representetation of the saved archetypes
as tree with the "parent" archetype as root and the contained archetypes and their children
as nodes and leafs.
Using the openEHR-EHR-OBSERVATION.body_temperature.v1 archetype as an example, we can see that
this archetypes can contain four archetypes as children: openEHR-EHR-CLUSTER.environmental_conditions.v1,
openEHR-EHR-CLUSTER.level_of_exertion.v1, openEHR-EHR-ELEMENT.last_normal_menstrual_period.v1 and
openEHR-EHR-ELEMENT.menstrual_cycle_day.v1. The last two archetypes will share the same path
of the parent archetype but this is irrelevant from the containment statement point of view.
Some of the structures that will be produced by this Archetype's instances are

.. code-block:: xml

   <archetype class="openEHR-EHR-OBSERVATION.body_temperature.v1">
     <archetype class="openEHR-EHR-CLUSTER.environmental_conditions.v1"/>
     <archetype class="openEHR-EHR-ELEMENT.last_normal_menstrual_period.v"/>
     <structure_id="1234551"/>
   </archetype>

   <archetype class="openEHR-EHR-OBSERVATION.body_temperature.v1">
     <archetype class="openEHR-EHR-CLUSTER.environmental_conditions.v1"/>
     <archetype class="openEHR-EHR-CLUSTER.level_of_exertion.v1"/>
     <structure_id="5431312"/>
   </archetype>

   <archetype class="openEHR-EHR-OBSERVATION.body_temperature.v1">
     <archetype class="openEHR-EHR-ELEMENT.menstrual_cycle_day.v1"/>
     <structure_id = "41551352"/>
   </archetype>

Each structure will have a different structure ID and each Archetype instance mapping one
of these structures will share the proper structure ID.
Resolving the statement means to map the CONTAINS statement to a XPATH query and use it to
retrieve all the structures that can satisy the statement.
If the query is

.. code-block:: none

   SELECT b/data[at0002]/events[at0003]/data[at0001]/items[at0004]/value AS Temperature
   FROM Ehr e
   CONTAINS OBSERVATION b[openEHR-EHR-OBSERVATION.body_temperature.v1]
   CONTAINS OBSERVATION o[openEHR-EHR-CLUSTER.level_of_exertion.v1]

we are going to retrieve the value of the temperature for all the instances of
openEHR-EHR-OBSERVATION.body_temperature.v1 that contains an instance of the
openEHR-EHR-CLUSTER.level_of_exertion.v1 archetype.
The CONTAINS statement will be automatically translated to the XPATH query

.. code-block:: none

   archetype[@class="openEHR-EHR-OBSERVATION.body_temperature.v1"]/archetype["openEHR-EHR-CLUSTER.level_of_exertion.v1"]/ancestor-or-self::archetype/structure_id

The query will return all the strucure_id nodes of the matching XML structures (using the previous XMLs as
example, only the <structure_id="5431312"/> will be returned); these IDs will be used by the driver to apply
a firt filter when selecting data so that only the records with the given strucure IDs will be retrieved
when permorming the query on the back-end engine.

Resolve AQL identified paths
----------------------------

In order to resolve WHERE and SELECT statements, paths expressed as AQL identified paths must be converted
to the full path that will be used to access data starting from the root archetype.
Using the following query as an example

.. code-block:: none

   SELECT o/items[at0009]/value as Phase
   FROM Ehr e
   CONTAINS OBSERVATION b[openEHR-EHR-OBSERVATION.body_temperature.v1]
   CONTAINS OBSERVATION o[openEHR-EHR-CLUSTER.level_of_exertion.v1]

it is necessary to properly map how the openEHR-EHR-CLUSTER.level_of_exertion.v1 archetype can be retrieved
from the openEHR-EHR-OBSERVATION.body_temperature.v1 archetype. The path that leads to the contained
archetype is

.. code-block:: none

   data[at0002]/events[at0003]/state[at0029]/items[at0057]

which means that the path o/items[at0009]/value need to be translated to
data[at0002]/events[at0003]/state[at0029]/items[at0057]/items[at0009]/value

This task will be performed by the Data Information System (currently not developed) but right now,
in order to have a mechanism that will allow to resolve AQL queries, path resolution can be a task
of the Index Service and can be performed by adding the full path from the root archetype to the
contained ones. The XML strucutures shown above can become like

.. code-block:: xml

   <archetype class="openEHR-EHR-OBSERVATION.body_temperature.v1">
     <archetype class="openEHR-EHR-CLUSTER.environmental_conditions.v1"
                path_from_root="data[at0002]/events[at0003]/state[at0029]/items[at0056]"/>
     <archetype class="openEHR-EHR-ELEMENT.last_normal_menstrual_period.v1"
                path_from_root="data[at0002]/events[at0003]/state[at0029]/items[at0058]"/>
     <structure_id="1234551"/>
   </archetype>

   <archetype class="openEHR-EHR-OBSERVATION.body_temperature.v1">
     <archetype class="openEHR-EHR-CLUSTER.environmental_conditions.v1"
                path_from_root="data[at0002]/events[at0003]/state[at0029]/items[at0056]"/>
     <archetype class="openEHR-EHR-CLUSTER.level_of_exertion.v1"
                path_from_root="data[at0002]/events[at0003]/state[at0029]/items[at0057]"/>
     <structure_id="5431312"/>
   </archetype>

   <archetype class="openEHR-EHR-OBSERVATION.body_temperature.v1">
     <archetype class="openEHR-EHR-ELEMENT.menstrual_cycle_day.v1"
                path_from_root="data[at0002]/events[at0003]/state[at0029]/items[at0058]"/>
     <structure_id = "41551352"/>
   </archetype>

When one or more strucure IDs are retrieved from the XML database, the variables like the b or the o
in the CONTAINS statement will be mapped to one or more path_from_root properties. Each path will be used
to construct the full paths used in SELECT and WHERE statements and to map these statements in a query
that can be used from the back-end engine.
If more than one path is returned, one example can be query with "generic" containment statements like

.. code-block:: none

   SELECT b/data[at0002]/events[at0003]/data[at0001]/items[at0004]/value AS Temperature
   FROM Ehr e
   CONTAINS COMPOSITION c
   CONTAINS OBSERVATION b[openEHR-EHR-OBSERVATION.body_temperature.v1]

this will result in more than one query executed on the back-end engine and the returned result
will be the sum of the results obtained from each query.
