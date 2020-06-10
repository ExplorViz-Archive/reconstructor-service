package net.explorviz.reconstructor.cassandra;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import java.io.IOException;
import java.util.List;
import net.explorviz.landscape.flat.Application;
import net.explorviz.landscape.flat.LandscapeRecord;
import net.explorviz.landscape.flat.Node;
import net.explorviz.reconstructor.SampleLoaderUtil;
import net.explorviz.reconstructor.peristence.PersistingException;
import net.explorviz.reconstructor.peristence.cassandra.DBHelper;
import net.explorviz.reconstructor.peristence.cassandra.LandscapeRecordRepository;
import net.explorviz.reconstructor.peristence.cassandra.mapper.LandscapeRecordMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link LandscapeRecordRepository}. The test are run against an in-memory
 * Cassandra database.
 */
class LandscapeRecordRepositoryTest extends CassandraTest {


  private LandscapeRecordMapper mapper;

  private LandscapeRecordRepository repository;

  @BeforeEach
  void setUp() {
    this.db.initialize();
    mapper = new LandscapeRecordMapper(this.db);
    this.repository = new LandscapeRecordRepository(this.db, this.mapper);
  }



  @Test
  void addNew() throws IOException, PersistingException {
    Node node = new Node("0.0.0.0", "localhost");
    Application app = new Application("SampleApplication", "1234", "java");
    String package$ = "net.explorviz.test";
    String class$ = "SampleClass";
    String method = "sampleMethod()";
    LandscapeRecord toAdd = LandscapeRecord.newBuilder()
        .setLandscapeToken("test_token")
        .setNode(node)
        .setApplication(app)
        .setTimestamp(1590231993321L)
        .setPackage$(package$)
        .setClass$(class$)
        .setMethod(method)
        .build();


    repository.add(toAdd);

    // Is inserted
    Assertions.assertEquals(1, countAll());
  }

  @Test
  void addWithoutToken() {
    Node node = new Node("0.0.0.0", "localhost");
    Application app = new Application("SampleApplication", "1234", "java");
    String package$ = "net.explorviz.test";
    String class$ = "SampleClass";
    String method = "sampleMethod()";
    LandscapeRecord toAdd = LandscapeRecord.newBuilder()
        .setLandscapeToken("")
        .setNode(node)
        .setApplication(app)
        .setTimestamp(1590231993321L)
        .setPackage$(package$)
        .setClass$(class$)
        .setMethod(method)
        .build();

    Assertions.assertThrows(PersistingException.class, () -> repository.add(toAdd));
  }

  @Test
  void addExisting() throws IOException, PersistingException {
    List<LandscapeRecord> records = SampleLoaderUtil.loadSampleApplication();
    for (LandscapeRecord r : records) {
      repository.add(r);
    }

    long before = countAll();
    // Should not add same record twice
    repository.add(records.get(0));
    long after = countAll();
    Assertions.assertEquals(before, after);
  }


  private long countAll() {
    String q = QueryBuilder.selectFrom(DBHelper.KEYSPACE_NAME, DBHelper.RECORDS_TABLE_NAME)
        .all().countAll().toString();
    return sess.execute(q).one().getLong(0);
  }


}
