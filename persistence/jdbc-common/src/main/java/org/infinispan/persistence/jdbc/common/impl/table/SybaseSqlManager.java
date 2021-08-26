package org.infinispan.persistence.jdbc.common.impl.table;

import java.util.List;

public class SybaseSqlManager extends GenericSqlManager {
   public SybaseSqlManager(String tableName, boolean namedParameters) {
      super(tableName, namedParameters);
   }

   @Override
   public String getUpsertStatement(List<String> keyColumns, List<String> allColumns) {
      StringBuilder upsert = new StringBuilder("MERGE INTO ").append(tableName);
      upsert.append(" AS t USING (SELECT ");
      appendStrings(upsert, allColumns, all -> parameterName(all) + " " + all, ", ");
      upsert.append(") AS tmp ON (");
      appendStrings(upsert, keyColumns, key -> "t." + key + " = tmp." + key, ", ");
      upsert.append(") WHEN MATCHED THEN UPDATE SET ");
      appendStrings(upsert, valueIterable(keyColumns, allColumns), value -> "t." + value + " = tmp." + value, ", ");
      upsert.append(" WHEN NOT MATCHED THEN INSERT VALUES (");
      appendStrings(upsert, allColumns, all -> "tmp." + all, ", ");
      upsert.append(')');
      return upsert.toString();
   }
}
