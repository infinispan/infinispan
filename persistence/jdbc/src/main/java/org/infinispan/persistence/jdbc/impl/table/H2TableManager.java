package org.infinispan.persistence.jdbc.impl.table;

import java.sql.Connection;

import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class H2TableManager extends AbstractTableManager {

   private static final Log log = LogFactory.getLog(H2TableManager.class, Log.class);

   H2TableManager(InitializationContext ctx, ConnectionFactory connectionFactory, JdbcStringBasedStoreConfiguration config, DbMetaData metaData, String cacheName) {
      super(ctx, connectionFactory, config, metaData, cacheName, log);
   }

   @Override
   protected String initUpsertRowSql() {
      if (dbMetadata.isSegmentedDisabled()) {
         return String.format("MERGE INTO %1$s (%2$s, %3$s, %4$s) KEY(%4$s) VALUES(?, ?, ?)", dataTableName,
               config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      } else {
         return String.format("MERGE INTO %1$s (%2$s, %3$s, %4$s, %5$s) KEY(%4$s) VALUES(?, ?, ?, ?)", dataTableName,
               config.dataColumnName(), config.timestampColumnName(), config.idColumnName(), config.segmentColumnName());
      }
   }

   @Override
   protected void dropIndex(Connection conn, String indexName) throws PersistenceException {
      String dropIndexDdl = String.format("DROP INDEX IF EXISTS  %s", getIndexName(dbMetadata.getMaxTableNameLength(), true, indexName));
      executeUpdateSql(conn, dropIndexDdl);
   }
}
