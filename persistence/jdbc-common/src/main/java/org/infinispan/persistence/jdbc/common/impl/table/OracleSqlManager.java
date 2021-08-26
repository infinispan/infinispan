package org.infinispan.persistence.jdbc.common.impl.table;

import java.util.List;
import java.util.function.Function;

public class OracleSqlManager extends GenericSqlManager {
   public OracleSqlManager(String tableName, boolean namedParameters) {
      super(tableName, namedParameters);
   }

   @Override
   public String getUpsertStatement(List<String> keyColumns, List<String> allColumns) {
      StringBuilder upsert = new StringBuilder("MERGE INTO ").append(tableName);
      upsert.append(" t USING (SELECT ");
      appendStrings(upsert, allColumns, all -> parameterName(all) + " " + all, ", ");
      upsert.append(" from dual) tmp ON (");
      appendStrings(upsert, keyColumns, key -> "t." + key + " = tmp." + key, ", ");
      upsert.append(") WHEN MATCHED THEN UPDATE SET ");
      appendStrings(upsert, valueIterable(keyColumns, allColumns), key -> "t." + key + " = tmp." + key, ", ");
      upsert.append(" WHEN NOT MATCHED THEN INSERT (");
      appendStrings(upsert, allColumns, Function.identity(), ", ");
      upsert.append(") VALUES (");
      appendStrings(upsert, allColumns, all -> "tmp." + all, ", ");
      upsert.append(')');
      return upsert.toString();
   }
}
