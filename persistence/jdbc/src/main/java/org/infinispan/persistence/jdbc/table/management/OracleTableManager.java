package org.infinispan.persistence.jdbc.table.management;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class OracleTableManager extends AbstractTableManager {

   private static final Log LOG = LogFactory.getLog(OracleTableManager.class, Log.class);

   private static final int MAX_INDEX_IDENTIFIER_SIZE = 30;
   private static final String INDEX_PREFIX = "IDX";

   OracleTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData) {
      super(connectionFactory, config, metaData, LOG);
   }

   @Override
   public boolean tableExists(Connection connection, TableName tableName) throws PersistenceException {
      Objects.requireNonNull(tableName, "table name is mandatory");
      ResultSet rs = null;
      try {
         DatabaseMetaData metaData = connection.getMetaData();
         String schemaPattern = tableName.getSchema() == null ? metaData.getUserName() : tableName.getSchema();
         rs = metaData.getTables(null, schemaPattern, tableName.getName(), new String[]{"TABLE"});
         return rs.next();
      } catch (SQLException e) {
         if (LOG.isTraceEnabled())
            LOG.tracef(e, "SQLException occurs while checking the table %s", tableName);
         return false;
      } finally {
         JdbcUtil.safeClose(rs);
      }
   }

   @Override
   protected boolean timestampIndexExists(Connection conn) throws PersistenceException {
      ResultSet rs = null;
      try {
         DatabaseMetaData meta = conn.getMetaData();
         rs = meta.getIndexInfo(null, null, getTableName().toString(), false, false);
         String indexName = getIndexName(false);
         while (rs.next()) {
            if (indexName.equalsIgnoreCase(rs.getString("INDEX_NAME"))) {
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

   @Override
   public String getIndexName(boolean withIdentifier) {
      int maxNameSize = MAX_INDEX_IDENTIFIER_SIZE - INDEX_PREFIX.length() - 1;
      if (withIdentifier) {
         maxNameSize -= 2;
      }
      String tableName = getTableName().toString().replace(identifierQuoteString, "");
      String truncatedName = tableName.length() > maxNameSize ? tableName.substring(0, maxNameSize) : tableName;
      String indexName = INDEX_PREFIX + "_" + truncatedName;
      if (withIdentifier) {
         return identifierQuoteString + indexName + identifierQuoteString;
      }
      return indexName;
   }

   @Override
   public String getUpsertRowSql() {
      if (upsertRowSql == null) {
         upsertRowSql = String.format("MERGE INTO %1$s t " +
                     "USING (SELECT ? %2$s, ? %3$s, ? %4$s from dual) tmp ON (t.%4$s = tmp.%4$s) " +
                     "WHEN MATCHED THEN UPDATE SET t.%2$s = tmp.%2$s, t.%3$s = tmp.%3$s " +
                     "WHEN NOT MATCHED THEN INSERT VALUES (tmp.%4$s, tmp.%2$s, tmp.%3$s)",
               this.getTableName(), config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      }
      return upsertRowSql;
   }
}
