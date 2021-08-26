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
class PostgresTableManager extends AbstractTableManager {

   private static final Log log = LogFactory.getLog(PostgresTableManager.class, Log.class);

   PostgresTableManager(InitializationContext ctx, ConnectionFactory connectionFactory, JdbcStringBasedStoreConfiguration config, DbMetaData metaData, String cacheName) {
      super(ctx, connectionFactory, config, metaData, cacheName, log);
   }

   @Override
   protected void dropIndex(Connection conn, String indexName) throws PersistenceException {
      String dropIndexDdl = String.format("DROP INDEX IF EXISTS  %s", getIndexName(true, indexName));
      executeUpdateSql(conn, dropIndexDdl);
   }

   @Override
   public String initUpdateRowSql() {
      return String.format("UPDATE %s SET %s = ? , %s = ? WHERE %s = cast(? as %s)",
            dataTableName, config.dataColumnName(), config.timestampColumnName(),
            config.idColumnName(), config.idColumnType());
   }

   @Override
   public String initSelectRowSql() {
      return String.format("SELECT %s, %s FROM %s WHERE %s = cast(? as %s)",
            config.dataColumnName(), config.idColumnName(), dataTableName,
            config.idColumnName(), config.idColumnType());
   }

   @Override
   public String initSelectIdRowSql() {
      return String.format("SELECT %s FROM %s WHERE %s = cast(? as %s)",
                                     config.idColumnName(), dataTableName, config.idColumnName(),
                                     config.idColumnType());
   }

   @Override
   public String initDeleteRowSql() {
      return String.format("DELETE FROM %s WHERE %s = cast(? as %s)",
            dataTableName, config.idColumnName(), config.idColumnType());
   }

   @Override
   public boolean isUpsertSupported() {
      // ON CONFLICT added in Postgres 9.5
      return super.isUpsertSupported() && (dbMetadata.getMajorVersion() >= 10 ||
            (dbMetadata.getMajorVersion() == 9 && dbMetadata.getMinorVersion() >= 5));
   }

   @Override
   public String initUpsertRowSql() {
      return String.format("%1$s ON CONFLICT (%2$s) DO UPDATE SET %3$s = EXCLUDED.%3$s, %4$s = EXCLUDED.%4$s",
               getInsertRowSql(), config.idColumnName(), config.dataColumnName(),
               config.timestampColumnName());
   }
}
