package net.explorviz.reconstructor.peristence.cassandra.mapper;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.explorviz.landscape.flat.Application;
import net.explorviz.reconstructor.peristence.cassandra.DBHelper;

public class ApplicationCodec extends MappingCodec<UdtValue, Application> {

  public ApplicationCodec(TypeCodec<UdtValue> innerCodec) {
    super(innerCodec, GenericType.of(Application.class));
  }

  @Nullable
  @Override
  protected Application innerToOuter(@Nullable UdtValue value) {
    String name = value.getString(DBHelper.COL_APP_NAME);
    String language = value.getString(DBHelper.COL_APP_LANGUAGE);
    String pid = value.getString(DBHelper.COL_APP_PID);
    return new Application(name, pid, language);
  }

  @Nullable
  @Override
  protected UdtValue outerToInner(@Nullable Application value) {
    UdtValue udtValue = ((UserDefinedType) getCqlType()).newValue();
    udtValue.setString(DBHelper.COL_APP_NAME, value.getName());
    udtValue.setString(DBHelper.COL_APP_LANGUAGE, value.getLanguage());
    udtValue.setString(DBHelper.COL_APP_PID, value.getPid());
    return udtValue;
  }
}
