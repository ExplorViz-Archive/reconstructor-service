package net.explorviz.reconstructor.stream;

import java.util.Properties;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.avro.landscape.flat.LandscapeRecord;
import net.explorviz.reconstructor.stream.util.EventThroughputLogger;
import net.explorviz.avro.EVSpan;
import net.explorviz.avro.Trace;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads traces from a kafka stream and converts them to {@link LandscapeRecord} by
 * extracting structural data out of each trace's spans.
 */
@ApplicationScoped
public class RecordExtractorStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordExtractorStream.class);
  private final EventThroughputLogger tpLogger;
  private final Properties streamsConfig;

  private Topology topology;

  private final KafkaHelper kafkaHelper;

  private SpanToRecordConverter converter;


  @Inject
  public RecordExtractorStream(KafkaHelper kafkaHelper,
                               SpanToRecordConverter converter) {
    this.converter = converter;
    this.kafkaHelper = kafkaHelper;
    this.streamsConfig = kafkaHelper.newDefaultStreamProperties();

    this.topology = buildTopology();
    this.tpLogger = new EventThroughputLogger(LOGGER);
  }

  public Topology getTopology() {
    return topology;
  }

  private Topology buildTopology() {

    StreamsBuilder builder = new StreamsBuilder();

    // Trace stream
    KStream<String, Trace> traceStream =
        builder.stream(kafkaHelper.getTopicTraces(), Consumed
            .with(Serdes.String(), kafkaHelper.getAvroValueSerde()));


    // Map to spans
    KStream<String, EVSpan> spanStream = traceStream.flatMapValues(Trace::getSpanList);


    // Map to records
    KStream<String, LandscapeRecord> recordKStream =
        spanStream.map((k, s) -> {
          LandscapeRecord record = converter.toRecord(s);
          return new KeyValue<>(record.getLandscapeToken(), record);
        });

    recordKStream.peek((k, v) -> LOGGER.info(v.toString()));

    //recordKStream.peek((k,v) -> tpLogger.logEvent());

    recordKStream
        .to(kafkaHelper.getTopicRecords(),
            Produced.with(Serdes.String(), kafkaHelper.getAvroValueSerde()));

    return builder.build();
  }


  public void startProcessor() {

    final KafkaStreams streams = new KafkaStreams(this.topology, streamsConfig);
    //streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }



}
