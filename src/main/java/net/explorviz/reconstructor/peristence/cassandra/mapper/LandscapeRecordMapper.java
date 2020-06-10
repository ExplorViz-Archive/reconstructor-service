package net.explorviz.reconstructor.peristence.cassandra.mapper;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.util.HashMap;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.avro.landscape.flat.Application;
import net.explorviz.avro.landscape.flat.LandscapeRecord;
import net.explorviz.avro.landscape.flat.Node;
import net.explorviz.reconstructor.peristence.cassandra.DBHelper;

@ApplicationScoped
public class LandscapeRecordMapper implements ValueMapper<LandscapeRecord> {

  private CodecRegistry codecRegistry;

  @Inject
  public LandscapeRecordMapper(DBHelper db) {
    this.codecRegistry = db.getCodecRegistry();
  }

  @Override
  public Map<String, Term> toMap(LandscapeRecord item) {
    Map<String, Term> map = new HashMap<>();
    map.put(DBHelper.COL_TOKEN, QueryBuilder.literal(item.getLandscapeToken()));
    map.put(DBHelper.COL_TIMESTAMP, QueryBuilder.literal(item.getTimestamp()));
    map.put(DBHelper.COL_NODE, QueryBuilder.literal(item.getNode(), codecRegistry));
    map.put(DBHelper.COL_APPLICATION, QueryBuilder.literal(item.getApplication(), codecRegistry));
    map.put(DBHelper.COL_PACKAGE, QueryBuilder.literal(item.getPackage$()));
    map.put(DBHelper.COL_CLASS, QueryBuilder.literal(item.getClass$()));
    map.put(DBHelper.COL_METHOD, QueryBuilder.literal(item.getMethod()));
    return map;
  }

  @Override
  public LandscapeRecord fromRow(Row row) {

    return LandscapeRecord.newBuilder()
        .setLandscapeToken(row.getString(DBHelper.COL_TOKEN))
        .setTimestamp(row.getLong(DBHelper.COL_TIMESTAMP))
        .setNode(row.get(DBHelper.COL_NODE, Node.class))
        .setApplication(row.get(DBHelper.COL_APPLICATION, Application.class))
        .setPackage$(row.getString(DBHelper.COL_PACKAGE))
        .setClass$(row.getString(DBHelper.COL_CLASS))
        .setMethod(row.getString(DBHelper.COL_METHOD))
        .build();

  }
}
