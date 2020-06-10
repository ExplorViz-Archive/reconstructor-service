package net.explorviz.reconstructor.stream;

import java.util.Properties;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.avro.landscape.flat.LandscapeRecord;
import net.explorviz.reconstructor.peristence.PersistingException;
import net.explorviz.reconstructor.peristence.Repository;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a stream processing sink for the extracted {@link LandscapeRecord}s.
 * The records are persisted in a database for further access.
 */
@ApplicationScoped
public class RecordPersistingProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordPersistingProcessor.class);

  private final KafkaHelper kafkaHelper;

  private final Repository<LandscapeRecord> recordRepo;

  @Inject
  public RecordPersistingProcessor(Repository<LandscapeRecord> repo, KafkaHelper kafkaHelper) {
    this.recordRepo = repo;
    this.kafkaHelper = kafkaHelper;

  }

  public StreamsBuilder addTopology(StreamsBuilder builder) {

    KStream<String, LandscapeRecord> recordStream =
        builder.stream(kafkaHelper.getTopicRecords(), Consumed
            .with(Serdes.String(), kafkaHelper.getAvroValueSerde()));

    recordStream.foreach((k, rec) -> {
      try {
        recordRepo.add(rec);
      } catch (PersistingException e) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Failed to persist an record from stream: {0}", e);
        }
      }
    });
    return builder;
  }



}