package org.infinispan.persistence.jdbc.impl.table;

import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class SQLiteTableManager extends AbstractTableManager {

   private static final Log log = LogFactory.getLog(SQLiteTableManager.class, Log.class);

   SQLiteTableManager(InitializationContext ctx, ConnectionFactory connectionFactory, JdbcStringBasedStoreConfiguration config, DbMetaData metaData, String cacheName) {
      super(ctx, connectionFactory, config, metaData, cacheName, log);
   }

   @Override
   public boolean isUpsertSupported() {
      // OR/ON CONFLICT introduced in 3.8.11
      return super.isUpsertSupported() && (dbMetadata.getMajorVersion() >= 4 ||
                                                 (dbMetadata.getMajorVersion() >= 3 && dbMetadata.getMinorVersion() >= 9));
   }

   @Override
   public String initUpsertRowSql() {
      if (dbMetadata.isSegmentedDisabled()) {
         return String.format("INSERT OR REPLACE INTO %s (%s, %s, %s) VALUES (?, ?, ?)",
               dataTableName, config.dataColumnName(), config.timestampColumnName(),
               config.idColumnName());
      } else {
         return String.format("INSERT OR REPLACE INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
               dataTableName, config.dataColumnName(), config.timestampColumnName(),
               config.idColumnName(), config.segmentColumnName());
      }
   }
}
