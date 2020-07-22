package net.explorviz.reconstructor.persistence.cassandra.mapper;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.explorviz.avro.landscape.flat.Application;
import net.explorviz.reconstructor.persistence.cassandra.DBHelper;

public class ApplicationCodec extends MappingCodec<UdtValue, Application> {

  public ApplicationCodec(final TypeCodec<UdtValue> innerCodec) {
    super(innerCodec, GenericType.of(Application.class));
  }

  @Nullable
  @Override
  protected Application innerToOuter(@Nullable final UdtValue value) {
    final String name = value.getString(DBHelper.COL_APP_NAME);
    final String language = value.getString(DBHelper.COL_APP_LANGUAGE);
    final String pid = value.getString(DBHelper.COL_APP_PID);
    return new Application(name, pid, language);
  }

  @Nullable
  @Override
  protected UdtValue outerToInner(@Nullable final Application value) {
    final UdtValue udtValue = ((UserDefinedType) this.getCqlType()).newValue();
    udtValue.setString(DBHelper.COL_APP_NAME, value.getName());
    udtValue.setString(DBHelper.COL_APP_LANGUAGE, value.getLanguage());
    udtValue.setString(DBHelper.COL_APP_PID, value.getPid());
    return udtValue;
  }
}
