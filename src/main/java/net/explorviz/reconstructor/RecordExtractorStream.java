package net.explorviz.reconstructor;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import java.util.Properties;
import javax.enterprise.context.ApplicationScoped;
import net.explorviz.landscape.flat.LandscapeRecord;
import net.explorviz.trace.EVSpan;
import net.explorviz.trace.Trace;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads traces from a kafka stream and converts them to {@link LandscapeRecord} by
 * extracting structural data out of each trace's spans.
 */
@ApplicationScoped
public class RecordExtractorStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordExtractorStream.class);

  private final Properties streamsConfig = new Properties();

  private Topology topology;

  private final SchemaRegistryClient registry;

  private SpanToRecordConverter converter;

  public RecordExtractorStream(SchemaRegistryClient registry, SpanToRecordConverter converter) {
    this.registry = registry;
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BROKER);
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConfig.APPLICATION_ID);

    this.topology = buildTopology();

    this.converter = converter;
  }

  public Topology getTopology() {
    return topology;
  }

  public Topology buildTopology() {

    StreamsBuilder builder = new StreamsBuilder();

    // Trace stream
    KStream<String, Trace> traceStream =
        builder.stream(KafkaConfig.TOPIC_TRACES, Consumed
            .with(Serdes.String(), getAvroValueSerde()));

    // Map to spans
    KStream<String, EVSpan> spanStream = traceStream.flatMapValues(Trace::getSpanList);

    // Map to records
    KStream<String, LandscapeRecord> recordKStream =
        spanStream.mapValues(s -> converter.toRecord(s));


    /*
    // TODO: Another Key? Landscape Token?
    // Use internal Stream?
    spanStream.to(KafkaConfig.TOPIC_COMPONENTS, Produced
        .with(Serdes.String(), getAvroValueSerde()));
    */

    return builder.build();
  }


  public void run() {

    final KafkaStreams streams = new KafkaStreams(this.topology, streamsConfig);
    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private <T extends SpecificRecord> SpecificAvroSerde<T> getAvroValueSerde() {
    final SpecificAvroSerde<T> valueSerde = new SpecificAvroSerde<>(registry);
    valueSerde.configure(
        Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"),
        false);
    return valueSerde;
  }

}
