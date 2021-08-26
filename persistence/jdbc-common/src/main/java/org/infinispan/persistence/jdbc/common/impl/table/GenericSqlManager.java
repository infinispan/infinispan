package org.infinispan.persistence.jdbc.common.impl.table;

import java.util.List;
import java.util.function.Function;

import org.infinispan.persistence.jdbc.common.SqlManager;

public class GenericSqlManager implements SqlManager {
   protected final String tableName;
   protected final boolean namedParameters;

   public GenericSqlManager(String tableName, boolean namedParameters) {
      this.tableName = tableName;
      this.namedParameters = namedParameters;
   }

   String parameterName(String columnName) {
      return namedParameters ? ":" + columnName : "?";
   }

   @Override
   public String getSelectStatement(List<String> keyColumns, List<String> allColumns) {
      StringBuilder select = new StringBuilder("SELECT ");
      appendStrings(select, allColumns, Function.identity(), ", ");
      select.append(" FROM ").append(tableName);
      select.append(" WHERE ");
      appendStrings(select, keyColumns, key -> key + " = " + parameterName(key), " AND ");
      return select.toString();
   }

   @Override
   public String getSelectAllStatement(List<String> allColumns) {
      StringBuilder selectAll = new StringBuilder("SELECT ");
      appendStrings(selectAll, allColumns, Function.identity(), ", ");
      selectAll.append(" FROM ").append(tableName);
      return selectAll.toString();
   }

   @Override
   public String getDeleteStatement(List<String> keyColumns) {
      StringBuilder delete = new StringBuilder("DELETE FROM ");
      delete.append(tableName);
      delete.append(" WHERE ");
      appendStrings(delete, keyColumns, key -> key + " = " + parameterName(key), " AND ");
      return delete.toString();
   }

   @Override
   public String getDeleteAllStatement() {
      return "DELETE FROM " + tableName;
   }

   protected Iterable<String> valueIterable(List<String> keyColumns, List<String> allColumns) {
      return () -> allColumns.stream().filter(all -> !keyColumns.contains(all)).iterator();
   }

   protected void appendStrings(StringBuilder sb, Iterable<String> strings, Function<String, String> valueConversion,
         String separator) {
      boolean isFirst = true;
      for (String columnName : strings) {
         if (!isFirst) {
            sb.append(separator);
         }
         sb.append(valueConversion.apply(columnName));
         isFirst = false;
      }
   }

   @Override
   public String getUpsertStatement(List<String> keyColumns, List<String> allColumns) {
      //      "MERGE INTO %1$s " +
      //      "USING (VALUES (?, ?, ?)) AS tmp (%2$s, %3$s, %4$s) " +
      //      "ON (%2$s = tmp.%2$s) " +
      //      "WHEN MATCHED THEN UPDATE SET %3$s = tmp.%3$s, %4$s = tmp.%4$s " +
      //      "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s)"
      StringBuilder upsert = new StringBuilder("MERGE INTO ").append(tableName);
      upsert.append(" USING (VALUES (");
      appendStrings(upsert, allColumns, this::parameterName, ", ");
      upsert.append(")) AS tmp (");
      appendStrings(upsert, allColumns, Function.identity(), ", ");
      upsert.append(") ON (");
      appendStrings(upsert, keyColumns, key -> key + " = tmp." + key, ", ");
      upsert.append(") WHEN MATCHED THEN UPDATE SET ");
      appendStrings(upsert, valueIterable(keyColumns, allColumns), value -> value + " = tmp." + value, ", ");
      upsert.append(" WHEN NOT MATCHED THEN INSERT (");
      appendStrings(upsert, allColumns, Function.identity(), ", ");
      upsert.append(") VALUES (");
      appendStrings(upsert, allColumns, all -> "tmp." + all, ", ");
      upsert.append(')');

      return upsert.toString();
   }

   @Override
   public String getSizeCommand() {
      return "SELECT COUNT(*) FROM " + tableName;
   }
}
