# SolrKafka Plugin

This project provides a plugin for importing documents from Kafka. This README describes
a setup process for Solr v8.x.

## Setup

### Copy Dependencies

Build the project with Maven and get the JAR produced
in target/. Copy that to <solr_install>/lib, which you'll have to create.

### Start Kafka

Start Kafka and Zookeeper (Zookeeper must be started before Kafka).

- Zookeeper: `<kafka_install>/bin/zookeeper-server-start.sh config/zookeeper.properties`
- Kafka: `<kafka_install>/bin/kafka-server-start.sh config/server.properties`

#### Create the Topic

To ensure a topic with that name has been created, run the following command:
`<kafka_install>/bin/kafka-topics.sh --create --topic <topic_name> --bootstrap-server localhost:9092`.

The topic name will be provided to the plugin in a later step.

### Start Solr

Start the Solr server with `bin/solr start -s <parent_directory>/solrkafka/src/conf/solr`. 
The `-s` flag allows you to point the
Solr server to a separate core/collection directory. In this case, we're pointing it to the conf/solr directory here.

Once Solr starts up, see if you're able to see it in the Solr UI.

If you're trying to debug in Solr, setup a remote JVM debugger and pass in the required parameters to Solr with
the `-a "<your_stuff_here>"` flag.

### Seed With Test Data

In the test directory of this project, there's a `SolrDocumentKafkaPublisher` class, which publishes documents 
that are processable by the SolrKafka plugin. Be sure to use that test to create documents to import, since special
characters are used in serialization/deserialization. If JSON documents are going to be imported instead, 
the `JsonKafkaPublisher` can be used instead, or custom documents can be created using your own method. JSON documents
can only be used with the request handler method, so make sure you use the correct publisher for your use case.

## Solr Request Handler Method

Copy produced jar and kafka-clients jar at root of project to lib dir.

Copy the following into the solrconfig.xml file, and make sure the `openSearcher` argument of `autoCommit` is set to true.

```xml
<!-- Creates the main request handler. This is available at the /solr/<collection-and-optional-core>/kafka. -->
<requestHandler name="/kafka" class="com.kmwllc.solr.solrkafka.handler.requesthandler.SolrKafkaRequestHandler">
    <lst name="defaults">
        <!-- The data type to read from Kafka. Available options are "solr" and "json". Default is "solr". -->
        <str name="incomingDataType">solr</str>
        <!-- How often the importer should wait before committing offsets back to Kafka. -->
        <str name="commitInterval">5000</str>
        <!-- True if the documents being imported should be added to all shards. False (default) if normal 
        importing rules should be applied. This must be false if Solr is not run in cloud mode. -->
        <str name="ignoreShardRouting">false</str>
        <!-- The Kafka broker. Required. -->
        <str name="kafkaBroker">localhost:9092</str>
        <!-- The topic to consume from in the form of a comma separated list with no spaces. Required. -->
        <str name="topicNames">testtopic</str>
        <!-- The max poll interval for Kafka. -->
        <str name="kafkaPollInterval">45000</str>
        <!-- Kafka consumer behavior when no previous offset is found. Acceptable values are "latest" or "beginning". -->
        <str name="autoOffsetResetConfig">beginning</str>
    </lst>
</requestHandler>
```

#### IgnoreShardRouting Caveats

- This mode cannot currently be run with TLOG replicas in your collection. If it is attempted, an error will be
returned with the response on startup.
- Solr legacy mode is not supported.
- The Kafka record ID should be the same as the document ID.

### Create Your Collection Schema

For tests, send this body in a request to `POST <solr_endpoint>/solr/<collection>/schema` to update your Solr collection's schema
if you want an explicitly defined schema:

> Note: the schema can also be dynamically created from input documents, so this step is not necessary anymore.

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

## Start Solr 

Start Solr normally, applying the `-a` flag if debugging is required.

## Request Handler Details (Starting the Handler)

The SolrKafka plugin can be started by performing a request to 
`GET <solr_endpoint>/solr/<collection-and-optional-core>/kafka`.

Note: The importer should only be running on one shard at a time. Running on multiple shards may cause documents
to be indexed multiple times, and can result in unpredictable behavior. Therefore, it's recommended that 
the request is sent to a specific core.

The importer can also take an action parameter, which will only work if the importer is running. If it's not running,
the importer will return with a message stating that it's not running. The `action` path parameter can take
one of the following values (if multiple values are provided, only the first is processed):

- `start`: The default action if `action` is omitted. Starts the importer. This is the only command that can be
  run if the importer is not already running at the time this request is made.
- `stop`: Shuts down the importer if it hasn't stopped on its own.
- `status`: Shows the status of the indexer

