package net.explorviz.reconstructor.peristence.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import java.net.InetSocketAddress;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Producer for preconfigured {@link CqlSession}.
 */
@ApplicationScoped
public class CassandraFactory {

  private CqlSession session;


  @Inject
  public CassandraFactory(
      @ConfigProperty(name = "explorviz.landscape.cassandra.host") String cassandraHost,
      @ConfigProperty(name = "explorviz.landscape.cassandra.port") int cassandraPort,
      @ConfigProperty(name = "explorviz.landscape.cassandra.datacenter") String datacenter,
      @ConfigProperty(name = "explorviz.landscape.cassandra.username") String username,
      @ConfigProperty(name = "explorviz.landscape.cassandra.password") String password) {
    // TODO read from config values
    CqlSessionBuilder builder = CqlSession.builder();
    builder.addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort));
    builder.withAuthCredentials(username, password);
    builder.withLocalDatacenter(datacenter);
    this.session = builder.build();
  }

  /**
   * Return a ready for use {@link CqlSession}
   */
  @Produces
  public CqlSession produceCqlSession() {
    return this.session;
  }


}
