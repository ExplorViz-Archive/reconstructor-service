package net.explorviz.reconstructor.peristence.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

/**
 * Producer for preconfigured {@link CqlSession}.
 */
public class CassandraFactory {

  private CqlSession session;


  public CassandraFactory() {
    // TODO read from config values
    CqlSessionBuilder builder = CqlSession.builder();
    this.session = builder.build();
  }

  /**
   * Return a ready for use {@link CqlSession}
   */
  @ApplicationScoped
  @Produces
  public CqlSession produceCqlSession() {
    return this.session;
  }


}
