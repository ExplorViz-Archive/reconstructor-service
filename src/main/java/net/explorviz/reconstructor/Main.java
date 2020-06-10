package net.explorviz.reconstructor;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import net.explorviz.reconstructor.peristence.cassandra.DBHelper;
import net.explorviz.reconstructor.stream.RecordExtractorStream;
import net.explorviz.reconstructor.stream.RecordsSink;

@QuarkusMain
public class Main implements QuarkusApplication {


  private DBHelper db;
  private RecordExtractorStream recordExtractorStream;
  private RecordsSink recordsSink;

  public Main(DBHelper db,
              RecordExtractorStream recordExtractorStream,
              RecordsSink recordsSink) {
    this.db = db;
    this.recordExtractorStream = recordExtractorStream;
    this.recordsSink = recordsSink;
  }

  @Override
  public int run(String... args) throws Exception {
    db.initialize();
    recordExtractorStream.startProcessor();
    recordsSink.startProcessor();
    Quarkus.waitForExit();
    return 0;
  }
}
