# SolrKafka Plugin

This project provides a plugin for importing documents from Kafka. This README describes
a setup process for Solr v8.x.

## Setup

### Copy Dependencies

Build the project with Maven (skipping tests with `mvn clean install -DskipTests` for now) and get the JAR produced
in target/. Copy that to <solr_install>/lib, which you'll have to create. You'll also need to copy over 
kafka-clients-<version>.jar, a dependency required by this plugin. You can find it at 
~/.m2/repository/org/apache/kafka/kafka-clients/<version>/<jar>. The solrconfig.xml file in 
conf/solr/collection1/conf/ contains references to these JAR files at that location.

### Start Kafka

Start Kafka and Zookeeper (Zookeeper must be started before Kafka).

- Zookeeper: `<kafka_install>/bin/zookeeper-server-start.sh config/zookeeper.properties`
- Kafka: `<kafka_install>/bin/kafka-server-start.sh config/server.properties`

#### Create the Topic

The `KafkaDataSource` uses the `query` property set in conf/solr/conf/kafka-data-config.xml to determine which
topic to use. The topic name will be provided to the plugin in your solrconfig.xml.

To ensure a topic with that name has been created, run the following command:
`<kafka_install>/bin/kafka-topics.sh --create --topic <topic_name> --bootstrap-server localhost:9092`.

### Start Solr

Start the Solr server with `bin/solr start -s <parent_directory>/solrkafka/src/conf/solr`. 
The `-s` flag allows you to point the
Solr server to a separate core/collection directory. In this case, we're pointing it to the conf/solr directory here.

Once Solr starts up, see if you're able to see it in the Solr UI.

If you're trying to debug in Solr, setup a remote JVM debugger and pass in the required parameters to Solr with
the `-a "<your_stuff_here>"` flag.

### Seed With Test Data

In the test directory of this project, there's a `SolrDocumentKafkaPublisher` class, which publishes documents 
that are processable by the SolrKafka plugin. Be sure to use that to create documents to import, since special
characters are used in serialization/deserialization.

## Solr Request Handler Method

Copy produced jar and kafka-clients jar at root of project to lib dir.

Copy the following into the solrconfig.xml file, and make sure the `openSearcher` argument of `autoCommit` is set to true.

```xml
<!-- Creates the main request handler. This is available at the /solr/<collection-and-optional-core>/kafka. -->
<requestHandler name="/kafka" class="com.kmwllc.solr.solrkafka.handler.requesthandler.SolrKafkaRequestHandler"
                startup="lazy">
    <lst name="defaults">
        <!-- The data type to read from Kafka. Available options are "solr" and "json". Default is "solr". -->
        <str name="incomingDataType">solr</str>
        <!-- The request handler to use. Simple is the default to be used if this is omitted. 
        Other types are not recommended. -->
        <str name="consumerType">simple</str>
        <!-- How often the importer should wait before committing offsets back to Kafka. -->
        <str name="commitInterval">5000</str>
        <!-- True if the documents being imported should be added to all shards. False (default) if normal 
        importing rules should be applied. This must be false if Solr is not run in cloud mode. -->
        <str name="ignoreShardRouting">false</str>
        <!-- The topic to consume from. Required. -->
        <str name="topicName">testtopic</str>
    </lst>
</requestHandler>
<!-- Creates the status request handler. Must be called on the same core that the /kafka handler was started on or no
useful information will be returned. -->
<requestHandler name="/kafka/status"
                class="com.kmwllc.solr.solrkafka.handler.requesthandler.SolrKafkaStatusRequestHandler"
                startup="lazy" />
<!-- Creates the distrib handler. This is only required if "ignoreShardRouting" is true. Handles inserting the 
documents on all cores except for the core that the /kafka importer is running on. -->
<requestHandler name="/kafka/distrib"
                class="com.kmwllc.solr.solrkafka.handler.requesthandler.DistributedCommandHandler"
                startup="lazy" />
```

### Create Your Collection Schema

> Note: as of now, the schema MUST be completed before the importer is run, or it will leave your collection schema
> in an inconsistent state.

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

## Start Solr 

Start Solr normally, applying the `-a` flag if debugging is required.

## Request Handler Details (Starting the Handler)

The SolrKafka plugin can be started by performing a request to 
`GET <solr_endpoint>/solr/<collection-and-optional-core>/kafka`.
The following optional query parameters are accepted:

- `fromBeginning`: `true` if the plugin should start from the beginning of the topic's history. Default is `false`.
- `exitAtEnd`: `true` if the plugin should exit once catching up to the end of the topic's history. Default is `false`.

If the importer is already running, then the importer will not do anything. If the request is made to a 
non-leader replica, then the importer will not start, will optionally start later if it becomes leader. A `leader` JSON
value is returned with the response, identifying the core that the request was made to as either a leader or a follower 
through a boolean value.

Note: The importer should only be running on one shard at a time. Running on multiple shards may cause documents
to be indexed multiple times, and can result in unpredictable behavior. Therefore, it's recommended that 
the request is sent to a specific core.

The importer can also take an action parameter, which will only work if the importer is running. If it's not running,
the importer will return with a message stating that it's not running. The `action` path parameter can take
one of the following values (if multiple values are provided, only the first is processed):

- `start`: The default action if `action` is omitted. Starts the importer. This is the only command that can be
  run if the importer is not already running at the time this request is made.
- `stop`: Shuts down the importer if it hasn't stopped on its own.
- `pause`: Pauses the Kafka consumer. The importer still runs, but no documents are received.
- `resumse`: Resumes the Kafka consumer if it was paused.
- `rewind`: Performs the same action as the `fromBeginning` parameter, but while the importer is running.

### Other Handlers

Two other handlers are provided with the Importer.

#### Status Handler

The status handler is available at `GET <solr_endpoint>/solr/<collection-and-optional-core>/kafka/status`.
When called, it will return a JSON identifying the status of the given core, along with the consumer group lag.

Note: This status handler must be called on the same core that the importer was started on, otherwise it will 
return incorrect information. It's best to keep track of the core that the importer was started on, and the importer
should be started using the explicit collection name with core identifier (e.g. 
`<collection>_<shard_number>_replica_<replica_number>`).

#### Distrib Handler

This is an internal handler used for distributing the document to all shards and replicas other than the core it was
initially indexed on. This handler __SHOULD NOT__ be used by anything other than this importer.

## Running the (old) DataImportHandler Method

### Run the Importer

In the Solr admin UI, navigate to "collection1", and go to the "Dataimport" section. Once you're ready to start, 
hit the "Execute" button, and you should see documents be imported when you hit refresh.

### Notes

If autocommit is turned on, the data import plugin can be run once, left on, and results will appear
after autocommit (and collection/core sync).
