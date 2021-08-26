package org.infinispan.persistence.jdbc.impl.table;

import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class MyTableOperations extends AbstractTableManager {
   private static final Log log = LogFactory.getLog(MyTableOperations.class, Log.class);

   MyTableOperations(InitializationContext ctx, ConnectionFactory connectionFactory, JdbcStringBasedStoreConfiguration config, DbMetaData metaData, String cacheName) {
      super(ctx, connectionFactory, config, metaData, cacheName, "`", log);
   }

   @Override
   public int getFetchSize() {
      return Integer.MIN_VALUE;
   }

   @Override
   public String initUpsertRowSql() {
         // Assumes that config.idColumnName is the primary key
      if (dbMetadata.isSegmentedDisabled()) {
         return String.format("%1$s ON DUPLICATE KEY UPDATE %2$s = VALUES(%2$s), %3$s = VALUES(%3$s)", getInsertRowSql(),
               config.dataColumnName(), config.timestampColumnName());
      } else {
         return String.format("%1$s ON DUPLICATE KEY UPDATE %2$s = VALUES(%2$s), %3$s = VALUES(%3$s), %4$s = VALUES(%4$s)", getInsertRowSql(),
               config.dataColumnName(), config.timestampColumnName(), config.segmentColumnName());
      }
   }
}
