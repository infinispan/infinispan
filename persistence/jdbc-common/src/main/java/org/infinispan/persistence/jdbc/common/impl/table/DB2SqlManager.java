package org.infinispan.persistence.jdbc.common.impl.table;

import java.util.List;
import java.util.function.Function;

public class DB2SqlManager extends GenericSqlManager {
   public DB2SqlManager(String tableName, boolean namedParameters) {
      super(tableName, namedParameters);
   }

   @Override
   public String getUpsertStatement(List<String> keyColumns, List<String> allColumns) {
      StringBuilder upsert = new StringBuilder("MERGE INTO ").append(tableName);
      upsert.append(" AS t USING (SELECT * FROM TABLE (VALUES(");
      appendStrings(upsert, allColumns, this::parameterName, ", ");
      upsert.append("))) AS tmp(");
      appendStrings(upsert, allColumns, Function.identity(), ", ");
      upsert.append(") ON ");
      appendStrings(upsert, allColumns, all -> "t." + all + " = tmp." + all, " AND ");
      upsert.append(" WHEN MATCHED THEN UPDATE SET (");
      appendStrings(upsert, valueIterable(keyColumns, allColumns), v -> "t." + v, ", ");
      upsert.append(") = (");
      appendStrings(upsert, valueIterable(keyColumns, allColumns), v -> "tmp." + v, ", ");
      upsert.append(") WHEN NOT MATCHED THEN INSERT (");
      appendStrings(upsert, allColumns, all -> "t." + all, ", ");
      upsert.append(") VALUES (");
      appendStrings(upsert, allColumns, all -> "tmp." + all, ", ");
      upsert.append(')');
      return upsert.toString();
   }
}
