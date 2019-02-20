package org.infinispan.persistence.jdbc.impl.table;

import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class MySQLTableManager extends AbstractTableManager {
   private static final Log LOG = LogFactory.getLog(MySQLTableManager.class, Log.class);

   MySQLTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData, String cacheName) {
      super(connectionFactory, config, metaData, cacheName, "`", LOG);
   }

   @Override
   public int getFetchSize() {
      return Integer.MIN_VALUE;
   }

   @Override
   public String initUpsertRowSql() {
         // Assumes that config.idColumnName is the primary key
      if (metaData.isSegmentedDisabled()) {
         return String.format("%1$s ON DUPLICATE KEY UPDATE %2$s = VALUES(%2$s), %3$s = VALUES(%3$s)", getInsertRowSql(),
               config.dataColumnName(), config.timestampColumnName());
      } else {
         return String.format("%1$s ON DUPLICATE KEY UPDATE %2$s = VALUES(%2$s), %3$s = VALUES(%3$s), %4$s = VALUES(%4$s)", getInsertRowSql(),
               config.dataColumnName(), config.timestampColumnName(), config.segmentColumnName());
      }
   }
}
