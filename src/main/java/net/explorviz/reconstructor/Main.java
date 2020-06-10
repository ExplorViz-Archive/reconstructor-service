package net.explorviz.reconstructor;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import net.explorviz.reconstructor.peristence.cassandra.DBHelper;
import net.explorviz.reconstructor.stream.ReconstructionStream;

@QuarkusMain
public class Main implements QuarkusApplication {


  private DBHelper db;
  private ReconstructionStream stream;

  public Main(DBHelper db, ReconstructionStream stream) {
    this.db = db;
    this.stream = stream;
  }

  @Override
  public int run(String... args) throws Exception {
    db.initialize();
    stream.getStream().cleanUp();
    stream.getStream().start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> stream.getStream().cleanUp()));
    Quarkus.waitForExit();
    return 0;
  }
}
