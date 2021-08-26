package org.infinispan.persistence.jdbc.common.impl.table;

import java.util.List;
import java.util.function.Function;

public class H2SqlManager extends GenericSqlManager {
   public H2SqlManager(String tableName, boolean namedParameters) {
      super(tableName, namedParameters);
   }

   @Override
   public String getUpsertStatement(List<String> keyColumns, List<String> allColumns) {
      StringBuilder upsert = new StringBuilder("MERGE INTO ").append(tableName);
      upsert.append(" (");
      appendStrings(upsert, allColumns, Function.identity(), ", ");
      upsert.append(") KEY(");
      appendStrings(upsert, keyColumns, Function.identity(), ", ");
      upsert.append(") VALUES(");
      appendStrings(upsert, allColumns, this::parameterName, ", ");
      upsert.append(")");
      return upsert.toString();
   }
}
