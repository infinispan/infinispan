package org.infinispan.persistence.jdbc.impl.table;

import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
class SQLServerTableManager extends AbstractTableManager {

   private static final Log LOG = LogFactory.getLog(MySQLTableManager.class, Log.class);

   SQLServerTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData) {
      super(connectionFactory, config, metaData, LOG);
   }

   @Override
   public String getUpsertRowSql() {
      if (upsertRowSql == null) {
         // As SQL Server does not handle a merge atomically, we must acquire the table lock here otherwise it's possible
         // for deadlocks to occur.
         if (metaData.isSegmentedDisabled()) {
            upsertRowSql = String.format("MERGE %1$s WITH (TABLOCK) " +
                        "USING (VALUES (?, ?, ?)) AS tmp (%2$s, %3$s, %4$s) " +
                        "ON (%1$s.%4$s = tmp.%4$s) " +
                        "WHEN MATCHED THEN UPDATE SET %2$s = tmp.%2$s, %3$s = tmp.%3$s " +
                        "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s);",
                  getTableName(), config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
         } else {
            upsertRowSql = String.format("MERGE %1$s WITH (TABLOCK) " +
                        "USING (VALUES (?, ?, ?, ?)) AS tmp (%2$s, %3$s, %4$s, %5$s) " +
                        "ON (%1$s.%4$s = tmp.%4$s) " +
                        "WHEN MATCHED THEN UPDATE SET %2$s = tmp.%2$s, %3$s = tmp.%3$s " +
                        "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s, %5$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s, %5$s);",
                  getTableName(), config.dataColumnName(), config.timestampColumnName(), config.idColumnName(),
                  config.segmentColumnName());
         }
      }
      return upsertRowSql;
   }

   @Override
   public boolean isStringEncodingRequired() {
      return metaData.getMajorVersion() <= 13;
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
