package net.explorviz.reconstructor.stream;

import java.util.Properties;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

@ApplicationScoped
public class ReconstructionStream {

  private KafkaHelper kHelper;

  private final Topology topology;

  private Properties props;

  private final KafkaStreams stream;

  private RecordPersistingProcessor recordPersistingProcessor;
  private RecordExtractorProcessor recordExtractorProcessor;

  @Inject
  public ReconstructionStream(KafkaHelper kHelper,
                              RecordExtractorProcessor recordExtractorProcessor,
                              RecordPersistingProcessor recordPersistingProcessor) {
    this.kHelper = kHelper;
    this.recordPersistingProcessor = recordPersistingProcessor;
    this.recordExtractorProcessor = recordExtractorProcessor;

    this.topology = buildTopology();
    props = kHelper.newDefaultStreamProperties();
    stream = new KafkaStreams(this.topology, this.props);
  }

  public KafkaStreams getStream() {
    return stream;
  }

  private Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();


    recordExtractorProcessor.addTopology(builder);
    recordPersistingProcessor.addTopology(builder);

    return builder.build();
  }

}
