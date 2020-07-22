package net.explorviz.reconstructor.stream;

import java.util.Properties;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.reconstructor.stream.util.KafkaHelper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

@ApplicationScoped
public class ReconstructionStream {

  private final KafkaHelper kHelper;

  private final Topology topology;

  private final Properties props;

  private final KafkaStreams stream;

  private final RecordPersistingProcessor recordPersistingProcessor;
  private final RecordExtractorProcessor recordExtractorProcessor;

  @Inject
  public ReconstructionStream(final KafkaHelper kHelper,
      final RecordExtractorProcessor recordExtractorProcessor,
      final RecordPersistingProcessor recordPersistingProcessor) {
    this.kHelper = kHelper;
    this.recordPersistingProcessor = recordPersistingProcessor;
    this.recordExtractorProcessor = recordExtractorProcessor;

    this.topology = this.buildTopology();
    this.props = kHelper.newDefaultStreamProperties();
    this.stream = new KafkaStreams(this.topology, this.props);
  }

  public KafkaStreams getStream() {
    return this.stream;
  }

  private Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();


    this.recordExtractorProcessor.addTopology(builder);
    this.recordPersistingProcessor.addTopology(builder);

    return builder.build();
  }

}
