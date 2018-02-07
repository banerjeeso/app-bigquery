#!/bin/bash

mvn spring-boot:run -Drun.arguments="--project=hd-www-dev,\
--stagingLocation=gs://hd-www-dev-catalog-data/staging,\
--dataflowJobFile=gs://hd-www-dev-catalog-data/templates/bigQueryTemplate,\
--tempLocation=gs://hd-www-dev-catalog-data/temp,\
--numWorkers=2,\
--network=internal,\
--maxNumWorkers=5,\
--zone=us-east1-c,\
--tableName=itemattr_20171107_0506,\
--outputFile=gs://hd-www-dev-catalog-data/extracts/catalog-dataflow-output/homedepot_catalog-dataflow,\
--runner=TemplatingDataflowPipelineRunner"



