package org.infinispan.persistence.jdbc.common.impl.table;

import java.util.List;
import java.util.function.Function;

public class SQLLiteSqlManager extends GenericSqlManager {
   public SQLLiteSqlManager(String tableName, boolean namedParameters) {
      super(tableName, namedParameters);
   }

   @Override
   public String getUpsertStatement(List<String> keyColumns, List<String> allColumns) {
      StringBuilder upsert = new StringBuilder("INSERT OR REPLACE INTO ").append(tableName);
      upsert.append(" (");
      appendStrings(upsert, allColumns, Function.identity(), ", ");
      upsert.append(") VALUES (");
      appendStrings(upsert, allColumns, this::parameterName, ", ");
      upsert.append(')');
      return upsert.toString();
   }
}
