package net.explorviz.reconstructor;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import net.explorviz.reconstructor.stream.ReconstructionStream;

@QuarkusMain
public class Main implements QuarkusApplication {


  private final ReconstructionStream stream;

  public Main(final ReconstructionStream stream) {
    this.stream = stream;
  }

  @Override
  public int run(final String... args) throws Exception {
    this.stream.getStream().cleanUp();
    this.stream.getStream().start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> this.stream.getStream().cleanUp()));
    Quarkus.waitForExit();
    return 0;
  }
}
