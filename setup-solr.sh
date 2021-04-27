#!/bin/zsh

#rm ~/Documents/solr/example/cloud/node1/solr/lib/solrkafka-0.0.1-SNAPSHOT.jar
#rm ~/Documents/solr/example/cloud/node2/solr/lib/solrkafka-0.0.1-SNAPSHOT.jar
cp -f target/solrkafka-0.0.1-SNAPSHOT.jar ~/Documents/solr/example/cloud/node1/solr/lib/
cp -f target/solrkafka-0.0.1-SNAPSHOT.jar ~/Documents/solr/example/cloud/node2/solr/lib/
cp -f target/solrkafka-0.0.1-SNAPSHOT.jar ~/Documents/solr/example/cloud/node3/solr/lib/
