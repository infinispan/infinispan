package org.infinispan.persistence.jdbc.impl.table;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.infinispan.persistence.jdbc.common.JdbcUtil;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class SybaseTableManager extends AbstractTableManager {
   private static final Log log = LogFactory.getLog(SybaseTableManager.class, Log.class);

   SybaseTableManager(InitializationContext ctx, ConnectionFactory connectionFactory, JdbcStringBasedStoreConfiguration config, DbMetaData metaData, String cacheName) {
      super(ctx, connectionFactory, config, metaData, cacheName, log);
   }

   @Override
   protected String getDropTimestampSql(String indexName) {
      return String.format("DROP INDEX %s.%s", dataTableName, getIndexName(true, indexName));
   }

   @Override
   protected String initUpdateRowSql() {
      return String.format("UPDATE %s SET %s = ? , %s = ? WHERE %s = convert(%s,?)",
            dataTableName, config.dataColumnName(), config.timestampColumnName(),
            config.idColumnName(), config.idColumnType());
   }

   @Override
   protected String initSelectRowSql() {
      return String.format("SELECT %s, %s FROM %s WHERE %s = convert(%s,?)",
                                   config.idColumnName(), config.dataColumnName(), dataTableName,
                                   config.idColumnName(), config.idColumnType());
   }

   @Override
   protected String initSelectIdRowSql() {
      return String.format("SELECT %s FROM %s WHERE %s = convert(%s,?)",
                                     config.idColumnName(), dataTableName, config.idColumnName(), config.idColumnType());
   }

   @Override
   protected String initDeleteRowSql() {
         return String.format("DELETE FROM %s WHERE %s = convert(%s,?)",
               dataTableName, config.idColumnName(), config.idColumnType());
   }

   @Override
   protected String initUpsertRowSql() {
      if (dbMetadata.isSegmentedDisabled()) {
         return String.format("MERGE INTO %1$s AS t " +
                     "USING (SELECT ? %2$s, ? %3$s, ? %4$s) AS tmp " +
                     "ON (t.%4$s = tmp.%4$s) " +
                     "WHEN MATCHED THEN UPDATE SET t.%2$s = tmp.%2$s, t.%3$s = tmp.%3$s " +
                     "WHEN NOT MATCHED THEN INSERT VALUES (tmp.%4$s, tmp.%2$s, tmp.%3$s)",
               dataTableName, config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      } else {
         return String.format("MERGE INTO %1$s AS t " +
                     "USING (SELECT ? %2$s, ? %3$s, ? %4$s, ? %5$s) AS tmp " +
                     "ON (t.%4$s = tmp.%4$s) " +
                     "WHEN MATCHED THEN UPDATE SET t.%2$s = tmp.%2$s, t.%3$s = tmp.%3$s " +
                     "WHEN NOT MATCHED THEN INSERT VALUES (tmp.%4$s, tmp.%2$s, tmp.%3$s, tmp.%5$s)",
               dataTableName, config.dataColumnName(), config.timestampColumnName(), config.idColumnName(),
               config.segmentColumnName());
      }
   }

   @Override
   protected boolean indexExists(String indexName, Connection conn) throws PersistenceException {
      ResultSet rs = null;
      try {
         DatabaseMetaData meta = conn.getMetaData();
         rs = meta.getIndexInfo(null, dataTableName.getSchema(), dataTableName.getName(), false, false);

         while (rs.next()) {
            String index = rs.getString("INDEX_NAME");
            if (index != null && indexName.equalsIgnoreCase(index.replaceAll("\"", ""))) {
               return true;
            }
         }
      } catch (SQLException e) {
         throw new PersistenceException(e);
      } finally {
         JdbcUtil.safeClose(rs);
      }
      return false;
   }
}
