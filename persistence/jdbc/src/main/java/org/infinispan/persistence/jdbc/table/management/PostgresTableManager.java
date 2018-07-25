package org.infinispan.persistence.jdbc.table.management;

import java.sql.Connection;

import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class PostgresTableManager extends AbstractTableManager {

   private static final Log LOG = LogFactory.getLog(PostgresTableManager.class, Log.class);

   PostgresTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData) {
      super(connectionFactory, config, metaData, LOG);
   }

   @Override
   protected void dropIndex(Connection conn, String indexName) throws PersistenceException {
      String dropIndexDdl = String.format("DROP INDEX IF EXISTS  %s", getIndexName(true, indexName));
      executeUpdateSql(conn, dropIndexDdl);
   }

   @Override
   public String getUpdateRowSql() {
      if (updateRowSql == null) {
         updateRowSql = String.format("UPDATE %s SET %s = ? , %s = ? WHERE %s = cast(? as %s)",
               getTableName(), config.dataColumnName(), config.timestampColumnName(),
               config.idColumnName(), config.idColumnType());
      }
      return updateRowSql;
   }

   @Override
   public String getSelectRowSql() {
      if (selectRowSql == null) {
         selectRowSql = String.format("SELECT %s, %s FROM %s WHERE %s = cast(? as %s)",
                                      config.idColumnName(), config.dataColumnName(), getTableName(),
                                      config.idColumnName(), config.idColumnType());
      }
      return selectRowSql;
   }

   @Override
   public String getSelectMultipleRowSql(int numberOfParams) {
      String selectCriteria = config.idColumnName() + " = cast(? as " + config.idColumnType() + ")";
      return getSelectMultipleRowSql(numberOfParams, selectCriteria);
   }

   @Override
   public String getSelectIdRowSql() {
      if (selectIdRowSql == null) {
         selectIdRowSql = String.format("SELECT %s FROM %s WHERE %s = cast(? as %s)",
                                        config.idColumnName(), getTableName(), config.idColumnName(),
                                        config.idColumnType());
      }
      return selectIdRowSql;
   }

   @Override
   public String getDeleteRowSql() {
      if (deleteRowSql == null) {
         deleteRowSql = String.format("DELETE FROM %s WHERE %s = cast(? as %s)",
                                      getTableName(), config.idColumnName(), config.idColumnType());
      }
      return deleteRowSql;
   }

   @Override
   public boolean isUpsertSupported() {
      // ON CONFLICT added in Postgres 9.5
      return super.isUpsertSupported() && (metaData.getMajorVersion() >= 10 ||
            (metaData.getMajorVersion() == 9 && metaData.getMinorVersion() >= 5));
   }

   @Override
   public String getUpsertRowSql() {
      if (upsertRowSql == null) {
         upsertRowSql = String.format("%1$s ON CONFLICT (%2$s) DO UPDATE SET %3$s = EXCLUDED.%3$s, %4$s = EXCLUDED.%4$s",
               getInsertRowSql(), config.idColumnName(), config.dataColumnName(),
               config.timestampColumnName());
      }
      return upsertRowSql;
   }
}
