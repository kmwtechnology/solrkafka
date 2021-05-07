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

If you're trying to debug in Solr, set up a remote JVM debugger and pass in the required parameters to Solr with
the `-a "<your_stuff_here>"` flag.

### Seed With Test Data

In the test directory of this project, there's a `SolrDocumentKafkaProducer` class, which publishes documents 
that are processable by the SolrKafka plugin. Be sure to use that test to create documents to import, since special
characters are used in serialization/deserialization. If JSON documents are going to be imported instead, 
the `JsonKafkaProducer` can be used instead, or custom documents can be created using your own method. JSON documents
can only be used with the request handler method, so make sure you use the correct producer for your use case.

## Solr Request Handler Method

Copy produced jar and kafka-clients jar at root of project to lib dir.

Copy the following into the solrconfig.xml file, and make sure the `openSearcher` argument of `autoCommit` is set to true.

```xml
<!-- Creates the main request handler. This is available at the /solr/<collection>/kafka. -->
<requestHandler name="/kafka" class="com.kmwllc.solr.solrkafka.handler.requesthandler.SolrKafkaRequestHandler">
    <lst name="defaults">
        <!-- The data type to read from Kafka. Available options are "solr" and "json". Default is "solr". -->
        <str name="incomingDataType">solr</str>
        <!-- The minimum amount of time that the importer should wait before committing offsets back to Kafka. -->
        <str name="commitInterval">5000</str>
        <!-- True if the documents being imported should be added to all shards. False (default) if normal 
        importing rules should be applied. This must be false if Solr is not run in cloud mode. -->
        <str name="ignoreShardRouting">false</str>
        <!-- The Kafka broker. Required. -->
        <str name="kafkaBroker">localhost:9092</str>
        <!-- The topic to consume from in the form of a comma separated list with no spaces. Required. -->
        <str name="topicNames">testtopic</str>
        <!-- The max poll interval for Kafka before the importer's consumer is evicted. -->
        <str name="kafkaPollInterval">45000</str>
        <!-- The max number of docs to be returned by a Kafka Consumer on poll. -->
        <str name="kafkaPollRecords">100</str>
        <!-- Kafka consumer behavior when no previous offset is found. Acceptable values are "latest" or "beginning". -->
        <str name="autoOffsetResetConfig">beginning</str>
    </lst>
</requestHandler>
```

#### Notes

- AllShardRouting/IgnoreShardRouting mode cannot currently be run with TLOG replicas in your collection. 
  If it is attempted, an error will be returned with the response on startup. TLOG replicas can be added 
  after the plugin is started, but there are no guarantees about whether the plugin will work if it is attempted.
- Solr legacy mode is not supported.
- The Kafka record ID should be the same as the document ID.
- If AllShardRouting/IgnoreShardRouting is stopped before reaching the latest records in each partition, each core's
  Kafka consumer may be at different offsets from each other. On restart, recovery will happen, and the cores should
  be synced with the leader. If the plugin is continued, then all cores will start from where they left off, and 
  some cores may end up receiving duplicate copies of documents (all cores should be in a consistent state once they
  reach the latest offsets though).
- If a node goes offline during indexing, and the live nodes have documents deleted, then the node that went offline
  will not end up deleteing the documents in recovery. Also, if the cluster is restarted, the node that went offline 
  *should* become the leader, but the documents that weren't deleted will not be redistributed to the nodes that deleted
  them in recovery.
- The plugin should be stopped during a shard split or unexpected behavior can occur. 
- If performing a shard split after using the plugin's AllShardRouting/IgnoreShardRouting mode, all documents must be
  reindexed, or the documents will be evenly distributed between the newly created shards rather than all being present
  on all shards
- Documents that are indexed using the plugin's AllShardRouting/IgnoreShardRouting mode must only be added/updated using
  the plugin in that mode. Otherwise, request reordering can cause updates to be improperly applied, and updates to the
  document will only be seen on the shard that the document would have originally been indexed on. All other shards will
  keep the original document.

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
`GET <solr_endpoint>/solr/<collection>/kafka`.

The importer can also take an action parameter, which will only work if the importer is running. If it's not running,
the importer will return with a message stating that it's not running. The `action` path parameter can take
one of the following values (if multiple values are provided, only the first is processed):

- `start`: The default action if `action` is omitted, and the plugin is not already running. 
  Starts the importer on all eligible cores.
- `stop`: Shuts down the importer on all eligible cores.
- `status`: The default action if `action` is omitted, and teh plugin is already running. 
  Shows the status of the indexer and Kafka consumer group lag information.
  
## Testing

In the test package (within the main sources directory), there are several classes that are used with testing the plugin.

- `docproducer.TestDocumentCreator`: an iterator that handles the creation of randomized `SolrDocument`s or production of 
  already-created `SolrDocument`s from a provided configuration. Used in the `JsonKafkaPub
- `docproducer.SolrDocumentKafkaProducer`: a class that publishes a set number of random documents serialized with the 
  `SolrDocumentSerializer` to a Kafka topic for testing.
- `docproducer.JsonKafkaProducer`: a class that publishes a set number of random documents serialized with the 
  `JsonSerializer` to a Kafka topic for testing.
- `SolrManager`: a class to manage and query Solr nodes. The main method of this class should only be run in a Docker 
  container, and it's used to start the Solr server. Other methods in this class are used within the other test classes.
- `SingleNodeTest`: a class that tests indexing in a non-cloud mode environment. Requires that the `singleNodeTest`
  core is already set up. This is done automatically in Docker with the docker-compose-test.yml compose file with the
  Dockerfile-test dockerfile.
  - Run this test by using the following command: 
    `docker exec -it <container> java -cp /var/solr/data/lib/* com.kmwllc.solr.solrkafka.test.SingleNodeTest -d`
    - Check the status code to see if it was successful: `echo $?`
- `MultiNodeTest`: a class that tests indexing in a cloud mode environment. Allows for back-to-back tests
  as long as different collection names (`--cname`s) are provided on each run. Does not require that the collection
  is set up beforehand. Uses the docker-compose-cloud-test.yml docker compose file with the Dockerfile-test dockerfile.
  - Run this test by using the following command:
    `docker exec -it <container> java -cp /var/solr/data/lib/* com.kmwllc.solr.solrkafka.test.MultiNodeTest -d <optional configs>`
    - Optional configs:
      - `-i`: runs the plugin in AllShardRouting/IgnoreShardRouting mode
      - `--cname COLL_NAME`: the name of the collection to-be-created, default is "cloudTest", different values are 
        required for each run with the same deployment
      - `--nrts NRTS` or `-r NRTS`: the number of NRT replicas to create, default is 2
      - `--tlogs TLOGS`: the number of TLOG replicas to create, default is 0
      - `--pulls PULLS`: the number of PULL replicas to create, default is 0
      - `-s SHARDS`: the number of shards to create, default is 2
      - `-k`: if provided, no new documents will be added to Kafka, required if running multiple times in the same 
        deployment after the first run
    - Check the status code to see if it was successful: `echo $?`
    - Note: after multiple runs, the volume may fill up completely and cause the cluster to go into an irrecoverable failure mode.
      This should not count as a test failure, so in cases where this occurs, restart the deployment and continue testing.
- `MultiNodeKillTest`: a class that tests indexing in a cloud mode environment where a node goes offline and comes back
  online. Can only be run once before the deployment needs to be restarted. Does not require that the 
  collection is set up beforehand. Uses the docker-compose-cloud-test.yml docker compose file with the Dockerfile-test dockerfile.
  - Run this test by using the following command:
    `docker exec -it <container> java -cp /var/solr/data/lib/* com.kmwllc.solr.solrkafka.test.MultiNodeKillTest -d [-i]`
    - `-i` is optional, runs the plugin in AllShardRouting/IgnoreShardRouting mode
    - Check the status code to see if it was successful: `echo $?`
