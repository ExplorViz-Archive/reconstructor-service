package net.explorviz.reconstructor.stream;

import java.util.Properties;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.avro.SpanStructure;
import net.explorviz.avro.landscape.flat.LandscapeRecord;
import net.explorviz.reconstructor.stream.util.EventThroughputLogger;
import net.explorviz.reconstructor.stream.util.KafkaHelper;
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

@ApplicationScoped
public class ReconstructionStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReconstructionStream.class);
  private final EventThroughputLogger tpLogger = new EventThroughputLogger(LOGGER);

  private final KafkaHelper kHelper;

  private final Topology topology;

  private final Properties props;

  private final SpanToRecordConverter converter;

  private final KafkaStreams stream;

  @Inject
  public ReconstructionStream(final KafkaHelper kHelper, final SpanToRecordConverter converter) {
    this.kHelper = kHelper;
    this.converter = converter;

    this.topology = this.buildTopology();
    this.props = kHelper.newDefaultStreamProperties();
    this.stream = new KafkaStreams(this.topology, this.props);
  }

  public KafkaStreams getStream() {
    return this.stream;
  }

  private Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();


    // Span stream
    final KStream<String, SpanStructure> spanStream =
        builder.stream(this.kHelper.getTopicSpans(), Consumed
            .with(Serdes.String(), this.kHelper.getAvroValueSerde()));

    // Map to records
    final KStream<String, LandscapeRecord> recordKStream =
        spanStream.map((k, s) -> {
          final LandscapeRecord record = this.converter.toRecord(s);
          tpLogger.logEvent();
          return new KeyValue<>(record.getLandscapeToken(), record);
        });

    recordKStream.to(this.kHelper.getTopicRecords(),
        Produced.with(Serdes.String(), this.kHelper.getAvroValueSerde()));

    return builder.build();
  }

}
