package net.explorviz.reconstructor.persistence.cassandra.mapper;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.explorviz.avro.landscape.flat.Node;
import net.explorviz.reconstructor.persistence.cassandra.DBHelper;

public class NodeCodec extends MappingCodec<UdtValue, Node> {

  public NodeCodec(final TypeCodec<UdtValue> innerCodec) {
    super(innerCodec, GenericType.of(Node.class));
  }

  @Nullable
  @Override
  protected Node innerToOuter(@Nullable final UdtValue value) {
    final String name = value.getString(DBHelper.COL_NODE_NAME);
    final String ip = value.getString(DBHelper.COL_NODE_IP_ADDRESS);
    return new Node(ip, name);
  }

  @Nullable
  @Override
  protected UdtValue outerToInner(@Nullable final Node value) {
    final UdtValue udtValue = ((UserDefinedType) this.getCqlType()).newValue();
    udtValue.setString(DBHelper.COL_NODE_NAME, value.getHostName());
    udtValue.setString(DBHelper.COL_NODE_IP_ADDRESS, value.getIpAddress());
    return udtValue;
  }
}
