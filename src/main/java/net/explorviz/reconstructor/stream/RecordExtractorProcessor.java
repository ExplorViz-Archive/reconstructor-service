package net.explorviz.reconstructor.stream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.avro.EVSpan;
import net.explorviz.avro.Trace;
import net.explorviz.avro.landscape.flat.LandscapeRecord;
import net.explorviz.reconstructor.stream.util.EventThroughputLogger;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
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
public class RecordExtractorProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordExtractorProcessor.class);
  private final EventThroughputLogger tpLogger;

  private final KafkaHelper kafkaHelper;

  private SpanToRecordConverter converter;


  @Inject
  public RecordExtractorProcessor(KafkaHelper kafkaHelper,
                                  SpanToRecordConverter converter) {
    this.converter = converter;
    this.kafkaHelper = kafkaHelper;

    this.tpLogger = new EventThroughputLogger(LOGGER);
  }


  public StreamsBuilder addTopology(StreamsBuilder builder) {

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

    return builder;
  }




}
