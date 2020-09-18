package net.explorviz.reconstructor.cassandra;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import java.io.IOException;
import java.util.List;
import net.explorviz.avro.landscape.flat.Application;
import net.explorviz.avro.landscape.flat.LandscapeRecord;
import net.explorviz.avro.landscape.flat.Node;
import net.explorviz.reconstructor.SampleLoaderUtil;
import net.explorviz.reconstructor.persistence.PersistingException;
import net.explorviz.reconstructor.persistence.cassandra.DBHelper;
import net.explorviz.reconstructor.persistence.cassandra.LandscapeRecordRepository;
import net.explorviz.reconstructor.persistence.cassandra.mapper.LandscapeRecordMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link LandscapeRecordRepository}. The test are run against an in-memory Cassandra
 * database.
 */
class LandscapeRecordRepositoryTest extends CassandraTest {


  private LandscapeRecordMapper mapper;

  private LandscapeRecordRepository repository;

  @BeforeEach
  void setUp() {
    this.db.initialize();
    this.mapper = new LandscapeRecordMapper(this.db);
    this.repository = new LandscapeRecordRepository(this.db, this.mapper);
  }



  @Test
  void addNew() throws IOException, PersistingException {
    // Setup a sample LandscapeRecord object to test with
    final Node node = new Node("0.0.0.0", "localhost");
    final Application app = new Application("SampleApplication", "1234", "java");
    final String package$ = "net.explorviz.test";
    final String class$ = "SampleClass";
    final String method = "sampleMethod()";
    final LandscapeRecord sampleRecord = LandscapeRecord.newBuilder()
        .setLandscapeToken("tok")
        .setTimestamp(System.currentTimeMillis())
        .setHashCode("3e90246268c9f235059878baa96eb9619fed65d9d4f573c01d1e30863c4afec4")
        .setNode(node)
        .setApplication(app)
        .setPackage$(package$)
        .setClass$(class$)
        .setMethod(method)
        .setHashCode("12345")
        .build();


    this.repository.add(sampleRecord);

    // Is inserted
    Assertions.assertEquals(1, this.countAll());
  }

  @Test
  void addWithoutToken() {
    // Setup a sample LandscapeRecord object to test with
    final Node node = new Node("0.0.0.0", "localhost");
    final Application app = new Application("SampleApplication", "1234", "java");
    final String package$ = "net.explorviz.test";
    final String class$ = "SampleClass";
    final String method = "sampleMethod()";
    final LandscapeRecord sampleRecord = LandscapeRecord.newBuilder()
        .setLandscapeToken("")
        .setTimestamp(System.currentTimeMillis())
        .setHashCode("3e90246268c9f235059878baa96eb9619fed65d9d4f573c01d1e30863c4afec4")
        .setNode(node)
        .setApplication(app)
        .setPackage$(package$)
        .setClass$(class$)
        .setHashCode("12345")
        .setMethod(method)
        .build();

    Assertions.assertThrows(PersistingException.class, () -> this.repository.add(sampleRecord));
  }

  @Test
  void addExisting() throws IOException, PersistingException {
    final List<LandscapeRecord> records = SampleLoaderUtil.loadSampleApplication();
    for (final LandscapeRecord r : records) {
      this.repository.add(r);
    }

    final long before = this.countAll();
    // Should not add same record twice
    this.repository.add(records.get(0));
    final long after = this.countAll();
    Assertions.assertEquals(before, after);
  }


  private long countAll() {
    final String q = QueryBuilder.selectFrom(DBHelper.KEYSPACE_NAME, DBHelper.RECORDS_TABLE_NAME)
        .all().countAll().toString();
    return this.sess.execute(q).one().getLong(0);
  }


}
