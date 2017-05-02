This is the Dockerfile to build the pyehr image 

**NOTE: the image is built only for MongoDB, ElasticSearch is not supported**

To build the image go in the docker directory and launch the command

`docker build -t <image_name> .`

To run a container, launch the command

`docker run -it -p 8984:8984 -p 8985:8985 -p 8081:8081 -p 27017:27017 -p 8090:8090 <image_name>`

This will start all the services (i.e., MongoDBdb, BaseX and PyEHR db and query services)

