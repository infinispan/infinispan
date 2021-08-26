package org.infinispan.persistence.jdbc.common.impl.table;

import java.util.List;
import java.util.function.Function;

public class SQLServerSqlManager extends GenericSqlManager {
   public SQLServerSqlManager(String tableName, boolean namedParameters) {
      super(tableName, namedParameters);
   }

   @Override
   public String getUpsertStatement(List<String> keyColumns, List<String> allColumns) {
      StringBuilder upsert = new StringBuilder("MERGE ").append(tableName);
      upsert.append(" WITH (TABLOCK) USING (VALUES (");
      appendStrings(upsert, allColumns, this::parameterName, ", ");
      upsert.append(")) AS tmp (");
      appendStrings(upsert, allColumns, Function.identity(), ", ");
      upsert.append(") ON (");
      appendStrings(upsert, keyColumns, key -> tableName + "." + key + " = tmp." + key, ", ");
      upsert.append(") WHEN MATCHED THEN UPDATE SET ");
      appendStrings(upsert, valueIterable(keyColumns, allColumns), value -> value + " = tmp." + value, ", ");
      upsert.append(" WHEN NOT MATCHED THEN INSERT (");
      appendStrings(upsert, allColumns, Function.identity(), ", ");
      upsert.append(") VALUES (");
      appendStrings(upsert, allColumns, all -> "tmp." + all, ", ");
      upsert.append(");");
      return upsert.toString();
   }
}
