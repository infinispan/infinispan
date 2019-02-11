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

   SQLiteTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData) {
      super(connectionFactory, config, metaData, LOG);
   }

   @Override
   public boolean isUpsertSupported() {
      // OR/ON CONFLICT introduced in 3.8.11
      return super.isUpsertSupported() && (metaData.getMajorVersion() >= 4 ||
                                                 (metaData.getMajorVersion() >= 3 && metaData.getMinorVersion() >= 9));
   }

   @Override
   public String getUpsertRowSql() {
      if (upsertRowSql == null) {
         if (metaData.isSegmentedDisabled()) {
            upsertRowSql = String.format("INSERT OR REPLACE INTO %s (%s, %s, %s) VALUES (?, ?, ?)",
                  getTableName(), config.dataColumnName(), config.timestampColumnName(),
                  config.idColumnName());
         } else {
            upsertRowSql = String.format("INSERT OR REPLACE INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
                  getTableName(), config.dataColumnName(), config.timestampColumnName(),
                  config.idColumnName(), config.segmentColumnName());
         }
      }
      return upsertRowSql;
   }
}
