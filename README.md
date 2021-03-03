# SolrKafka Plugin

This project provides a plugin for importing documents from Kafka via the DataImportHandler. This README describes
a setup process for Solr v8.x.

## Copy Dependencies

Build the project with Maven (skipping tests with `mvn clean install -DskipTests` for now) and get the JAR produced
in target/. Copy that to <solr_install>/lib, which you'll have to create. You'll also need to copy over 
kafka-clients-<version>.jar, a dependency required by this plugin. You can find it at 
~/.m2/repository/org/apache/kafka/kafka-clients/<version>/<jar>. The solrconfig.xml file in 
conf/solr/collection1/conf/ contains references to these JAR files at that location.

## Start Kafka

Start Kafka and Zookeeper (Zookeeper must be started before Kafka).

- Zookeeper: `<kafka_install>/bin/zookeeper-server-start.sh config/zookeeper.properties`
- Kafka: `<kafka_install>/bin/kafka-server-start.sh config/server.properties`

### Create the Topic

The `KafkaDataSource` uses the `query` property set in conf/solr/conf/kafka-data-config.xml to determine which
topic to use. The default is "testtopic". 

To ensure a topic with that name has been created, run the following command:
`<kafka_install>/bin/kafka-topics.sh --create --topic <topic_name> --bootstrap-server localhost:9092`.

## Start Solr

Start the Solr server with `bin/solr start -s <parent_directory>/solrkafka/src/conf/solr`. 
The `-s` flag allows you to point the
Solr server to a separate core/collection directory. In this case, we're pointing it to the conf/solr directory here.

Once Solr starts up, see if you're able to see it in the Solr UI.

If you're trying to debug in Solr, setup a remote JVM debugger and pass in the required parameters to Solr with
the `-a "<your_stuff_here>"` flag.

## Seed With Test Data

In the test directory of this project, there's a `SolrDocumentKafkaPublisher` class, which publishes documents 
that are processable by the SolrKafka plugin. Be sure to use that to create documents to import, since special
characters are used in serialization/deserialization.

## Run the Importer

In the Solr admin UI, navigate to "collection1", and go to the "Dataimport" section. Once you're ready to start, 
hit the "Execute" button, and you should see documents be imported when you hit refresh.

## Notes

If autocommit is turned on, the data import plugin can be run once, left on, and results will appear
after autocommit (and collection/core sync).
