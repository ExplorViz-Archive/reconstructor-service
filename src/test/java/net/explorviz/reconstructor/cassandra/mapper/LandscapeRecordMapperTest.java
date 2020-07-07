package net.explorviz.reconstructor.cassandra.mapper;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.DefaultLiteral;
import java.util.Map;
import net.explorviz.avro.landscape.flat.Application;
import net.explorviz.avro.landscape.flat.LandscapeRecord;
import net.explorviz.avro.landscape.flat.Node;
import net.explorviz.reconstructor.cassandra.CassandraTest;
import net.explorviz.reconstructor.peristence.cassandra.DBHelper;
import net.explorviz.reconstructor.peristence.cassandra.mapper.LandscapeRecordMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

class LandscapeRecordMapperTest extends CassandraTest {

  private LandscapeRecord sampleRecord;

  private LandscapeRecordMapper mapper;

  @Mock
  private Row mockRow;

  @BeforeEach
  void setUp() {
    db.initialize();
    // Setup a sample LandscapeRecord object to test with
    Node node = new Node("0.0.0.0", "localhost");
    Application app = new Application("SampleApplication", "1234", "java");
    String package$ = "net.explorviz.test";
    String class$ = "SampleClass";
    String method = "sampleMethod()";
    sampleRecord = LandscapeRecord.newBuilder()
        .setLandscapeToken("tok")
        .setTimestamp(System.currentTimeMillis())
        .setNode(node)
        .setApplication(app)
        .setPackage$(package$)
        .setClass$(class$)
        .setMethod(method)
        .build();

    mapper = new LandscapeRecordMapper(this.db);
  }

  @Test
  void toMap() {
    Map<String, Term> map = mapper.toMap(sampleRecord);

    DefaultLiteral<Long> timestamp = (DefaultLiteral<Long>) map.get(DBHelper.COL_TIMESTAMP);
    DefaultLiteral<String> token = (DefaultLiteral<String>) map.get(DBHelper.COL_TOKEN);
    DefaultLiteral<String> methodName = (DefaultLiteral<String>) map.get(DBHelper.COL_METHOD);

    DefaultLiteral<String> packageName = (DefaultLiteral<String>) map.get(DBHelper.COL_PACKAGE);
    DefaultLiteral<String> className = (DefaultLiteral<String>) map.get(DBHelper.COL_CLASS);
    DefaultLiteral<Node> node = (DefaultLiteral<Node>) map.get(DBHelper.COL_NODE);
    DefaultLiteral<Application> application =
        (DefaultLiteral<Application>) map.get(DBHelper.COL_APPLICATION);


    Assertions.assertEquals(sampleRecord.getLandscapeToken(), token.getValue());
    Assertions.assertEquals(sampleRecord.getTimestamp(), timestamp.getValue());
    Assertions.assertEquals(sampleRecord.getPackage$(), packageName.getValue());
    Assertions.assertEquals(sampleRecord.getMethod(), methodName.getValue());
    Assertions.assertEquals(sampleRecord.getClass$(), className.getValue());
    Assertions.assertEquals(sampleRecord.getNode(), node.getValue());
    Assertions.assertEquals(sampleRecord.getApplication(), application.getValue());
  }


  @Test
  void fromRow() {
    // Tests the conversion but not the codecs...
    mockRow = Mockito.mock(Row.class);
    Mockito.when(mockRow.getString(DBHelper.COL_TOKEN))
        .thenReturn(sampleRecord.getLandscapeToken());
    Mockito.when(mockRow.getLong(DBHelper.COL_TIMESTAMP)).thenReturn(sampleRecord.getTimestamp());
    Mockito.when(mockRow.getString(DBHelper.COL_METHOD)).thenReturn(sampleRecord.getMethod());
    Mockito.when(mockRow.getString(DBHelper.COL_PACKAGE)).thenReturn(sampleRecord.getPackage$());
    Mockito.when(mockRow.getString(DBHelper.COL_CLASS)).thenReturn(sampleRecord.getClass$());
    Mockito.when(mockRow.get(DBHelper.COL_NODE, Node.class)).thenReturn(sampleRecord.getNode());
    Mockito.when(mockRow.get(DBHelper.COL_APPLICATION, Application.class))
        .thenReturn(sampleRecord.getApplication());

    LandscapeRecord got = mapper.fromRow(mockRow);
    Assertions.assertEquals(sampleRecord, got);

  }

}
