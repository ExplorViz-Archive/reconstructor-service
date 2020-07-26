package net.explorviz.reconstructor.persistence.cassandra;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import net.explorviz.avro.landscape.flat.LandscapeRecord;
import net.explorviz.reconstructor.persistence.PersistingException;
import net.explorviz.reconstructor.persistence.Repository;
import net.explorviz.reconstructor.persistence.cassandra.mapper.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra-backed repository to access and save {@link LandscapeRecord} entities.
 */
@ApplicationScoped
public class LandscapeRecordRepository implements Repository<LandscapeRecord> {

  private static final Logger LOGGER = LoggerFactory.getLogger(LandscapeRecordRepository.class);

  private final DBHelper db;
  private final ValueMapper<LandscapeRecord> mapper;

  /**
   * Create a new repository for accessing {@link LandscapeRecord} object.
   *
   * @param db the backing Casandra db
   */
  public LandscapeRecordRepository(final DBHelper db, final ValueMapper<LandscapeRecord> mapper) {
    this.db = db;
    db.initialize();
    this.mapper = mapper;
  }


  @Override
  public void add(final LandscapeRecord item) throws PersistingException {
    final Map<String, Term> values = this.mapper.toMap(item);
    final SimpleStatement insertStmt =
        QueryBuilder.insertInto(DBHelper.KEYSPACE_NAME, DBHelper.RECORDS_TABLE_NAME)
            .values(values)
            .build();
    try {
      this.db.getSession().execute(insertStmt);
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Inserted new record for landscape token {}", item.getLandscapeToken());
      }
    } catch (final AllNodesFailedException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Failed to insert new record: Database unreachable");
      }
      throw new PersistingException(e);
    } catch (QueryExecutionException | QueryValidationException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Failed to insert new record: {0}", e.getCause());
      }
      throw new PersistingException(e);
    }
  }

}