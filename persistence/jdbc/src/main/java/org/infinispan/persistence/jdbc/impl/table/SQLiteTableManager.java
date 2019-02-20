package org.infinispan.persistence.jdbc.impl.table;

import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class SQLiteTableManager extends AbstractTableManager {

   private static final Log LOG = LogFactory.getLog(SQLiteTableManager.class, Log.class);

   SQLiteTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData, String cacheName) {
      super(connectionFactory, config, metaData, cacheName, LOG);
   }

   @Override
   public boolean isUpsertSupported() {
      // OR/ON CONFLICT introduced in 3.8.11
      return super.isUpsertSupported() && (metaData.getMajorVersion() >= 4 ||
                                                 (metaData.getMajorVersion() >= 3 && metaData.getMinorVersion() >= 9));
   }

   @Override
   public String initUpsertRowSql() {
      if (metaData.isSegmentedDisabled()) {
         return String.format("INSERT OR REPLACE INTO %s (%s, %s, %s) VALUES (?, ?, ?)",
               tableName, config.dataColumnName(), config.timestampColumnName(),
               config.idColumnName());
      } else {
         return String.format("INSERT OR REPLACE INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
               tableName, config.dataColumnName(), config.timestampColumnName(),
               config.idColumnName(), config.segmentColumnName());
      }
   }
}
