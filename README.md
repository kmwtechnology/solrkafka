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


# New Impl

Copy produced jar and kafka-clients jar at root of project to lib dir.

Copy the following into the solrconfig.xml file, and make sure the `openSearcher` argument of `autoCommit` is set to true.

```xml

<requestHandler name="/kafka" class="com.kmwllc.solr.solrkafka.handlers.requesthandlers.SolrKafkaRequestHandler"
                startup="lazy">
<lst name="defaults">
    <str name="incomingDataType">solr</str>
    <str name="consumerType">simple</str>
    <str name="commitInterval">5000</str>
</lst>
</requestHandler>
<requestHandler name="/kafka/status"
                class="com.kmwllc.solr.solrkafka.handlers.requesthandlers.SolrKafkaStatusRequestHandler"
                startup="lazy"/>
```

For tests, send this body in a request to `POST <solr_endpoint>/solr/<collection>/schema` to update your Solr collection's schema:

```json
{
    "add-field": [
        {
            "name": "title",
            "type": "text_general"
        },
        {
            "name": "date",
            "type": "text_general"
        }
    ]
}
```

This plugin works similarly to the previous version, however, Solr is started normally (ignore everything in the
Start Solr section except for the `-a` flag if desired), and the plugin is started as described below.

The SolrKafka plugin can be started by performing a request to `GET <solr_endpoint>/solr/<collection>/kafka`.
The following query parameters are accepted:

- `fromBeginning`: `true` if the plugin should start from the beginning of the topic's history. Default is `false`.
- `exitAtEnd`: `true` if the plugin should exit once catching up to the end of the topic's history. Default is `false`.

A status endpoint is also available at `GET <solr_endpoint>/solr/<collection>/kafka/status`.
