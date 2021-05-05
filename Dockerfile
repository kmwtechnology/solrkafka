FROM maven:3-openjdk-11 AS builder

COPY . /solrkafka
WORKDIR /solrkafka
RUN mvn -B verify --file pom.xml

FROM solr:8.8.1

COPY --from=builder /solrkafka/target/solrkafka-*.jar server/solr/lib/
