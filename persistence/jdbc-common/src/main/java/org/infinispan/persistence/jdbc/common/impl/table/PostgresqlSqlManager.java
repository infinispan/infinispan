package org.infinispan.persistence.jdbc.common.impl.table;

import java.util.List;
import java.util.function.Function;

public class PostgresqlSqlManager extends GenericSqlManager {
   public PostgresqlSqlManager(String tableName, boolean namedParameters) {
      super(tableName, namedParameters);
   }

   @Override
   public String getUpsertStatement(List<String> keyColumns, List<String> allColumns) {
      StringBuilder upsert = new StringBuilder("INSERT INTO ").append(tableName);
      upsert.append(" (");
      appendStrings(upsert, allColumns, Function.identity(), ", ");
      upsert.append(") VALUES (");
      appendStrings(upsert, allColumns, this::parameterName, ", ");
      upsert.append(") ON CONFLICT (");
      appendStrings(upsert, keyColumns, Function.identity(), ", ");
      upsert.append(") DO UPDATE SET ");
      appendStrings(upsert, valueIterable(keyColumns, allColumns), value -> value + " = EXCLUDED." + value, ", ");
      return upsert.toString();
   }
}
