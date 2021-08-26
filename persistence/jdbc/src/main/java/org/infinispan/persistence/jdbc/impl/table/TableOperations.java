package org.infinispan.persistence.jdbc.impl.table;

import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
class TableOperations extends AbstractTableManager {

   private static final Log log = LogFactory.getLog(MyTableOperations.class, Log.class);

   TableOperations(InitializationContext ctx, ConnectionFactory connectionFactory, JdbcStringBasedStoreConfiguration config, DbMetaData metaData, String cacheName) {
      super(ctx, connectionFactory, config, metaData, cacheName, log);
   }

   @Override
   public String initUpsertRowSql() {
      // As SQL Server does not handle a merge atomically, we must acquire the table lock here otherwise it's possible
         // for deadlocks to occur.
      if (dbMetadata.isSegmentedDisabled()) {
         return String.format("MERGE %1$s WITH (TABLOCK) " +
                     "USING (VALUES (?, ?, ?)) AS tmp (%2$s, %3$s, %4$s) " +
                     "ON (%1$s.%4$s = tmp.%4$s) " +
                     "WHEN MATCHED THEN UPDATE SET %2$s = tmp.%2$s, %3$s = tmp.%3$s " +
                     "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s);",
               dataTableName, config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      } else {
         return String.format("MERGE %1$s WITH (TABLOCK) " +
                     "USING (VALUES (?, ?, ?, ?)) AS tmp (%2$s, %3$s, %4$s, %5$s) " +
                     "ON (%1$s.%4$s = tmp.%4$s) " +
                     "WHEN MATCHED THEN UPDATE SET %2$s = tmp.%2$s, %3$s = tmp.%3$s " +
                     "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s, %5$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s, %5$s);",
               dataTableName, config.dataColumnName(), config.timestampColumnName(), config.idColumnName(),
               config.segmentColumnName());
      }
   }

   @Override
   protected String initSelectOnlyExpiredRowsSql() {
      String loadAll = String.format("%s WITH (UPDLOCK)", getLoadAllRowsSql());
      return String.format("%1$s WHERE %2$s < ? AND %2$s > 0", loadAll, config.timestampColumnName());
   }

   @Override
   public boolean isStringEncodingRequired() {
      return dbMetadata.getMajorVersion() <= 13;
   }

   @Override
   public String encodeString(String string) {
      char[] srcChars = string.toCharArray();
      if (srcChars.length > 0 && srcChars[0] == '\uFEFF') {
         char[] chars = new char[srcChars.length - 1];
         string.getChars(1, string.toCharArray().length, chars, 0);
         return new String(chars);
      }
      return string;
   }
}
