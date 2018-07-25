package org.infinispan.persistence.jdbc.table.management;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import org.infinispan.commons.io.ByteBuffer;
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
   protected boolean indexExists(String indexName, Connection conn) throws PersistenceException {
      ResultSet rs = null;
      try {
         DatabaseMetaData meta = conn.getMetaData();
         rs = meta.getIndexInfo(null, null, getTableName().toString(), false, false);
         while (rs.next()) {
            String index = rs.getString("INDEX_NAME");
            if (indexName.equalsIgnoreCase(index)) {
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
   public String getIndexName(boolean withIdentifier, String indexExt) {
      if (indexExt.equals(timestampIndexExt)) {
         // Timestamp for Oracle began with IDX, to keep backwards compatible we have to keep using that
         indexExt = INDEX_PREFIX;
      }
      int maxNameSize = MAX_INDEX_IDENTIFIER_SIZE - indexExt.length() - 1;
      String tableName = getTableName().toString().replace(identifierQuoteString, "");
      String truncatedName = tableName.length() > maxNameSize ? tableName.substring(0, maxNameSize) : tableName;
      String indexName = indexExt + "_" + truncatedName;
      if (withIdentifier) {
         return identifierQuoteString + indexName + identifierQuoteString;
      }
      return indexName;
   }

   protected String getDropTimestampSql() {
      return String.format("DROP INDEX %s", getIndexName(true, timestampIndexExt));
   }

   @Override
   public String getInsertRowSql() {
      if (insertRowSql == null) {
         if (metaData.isSegmentedDisabled()) {
            insertRowSql = String.format("INSERT INTO %s (%s,%s,%s) VALUES (?,?,?)", getTableName(),
                  config.idColumnName(), config.timestampColumnName(), config.dataColumnName());
         } else {
            insertRowSql = String.format("INSERT INTO %s (%s,%s,%s,%s) VALUES (?,?,?,?)", getTableName(),
                  config.idColumnName(), config.timestampColumnName(), config.dataColumnName(), config.segmentColumnName());
         }
      }
      return insertRowSql;
   }

   @Override
   public String getUpdateRowSql() {
      if (updateRowSql == null) {
         updateRowSql = String.format("UPDATE %s SET %s = ? , %s = ? WHERE %s = ?", getTableName(),
               config.timestampColumnName(), config.dataColumnName(), config.idColumnName());
      }
      return updateRowSql;
   }

   @Override
   public String getUpsertRowSql() {
      if (upsertRowSql == null) {
         if (metaData.isSegmentedDisabled()) {
            upsertRowSql = String.format("MERGE INTO %1$s t " +
                        "USING (SELECT ? %2$s, ? %3$s, ? %4$s from dual) tmp ON (t.%2$s = tmp.%2$s) " +
                        "WHEN MATCHED THEN UPDATE SET t.%3$s = tmp.%3$s, t.%4$s = tmp.%4$s " +
                        "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s)",
                  this.getTableName(), config.idColumnName(), config.timestampColumnName(), config.dataColumnName());
         } else {
            upsertRowSql = String.format("MERGE INTO %1$s t " +
                        "USING (SELECT ? %2$s, ? %3$s, ? %4$s, ? %5$s from dual) tmp ON (t.%2$s = tmp.%2$s) " +
                        "WHEN MATCHED THEN UPDATE SET t.%3$s = tmp.%3$s, t.%4$s = tmp.%4$s " +
                        "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s, %5$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s, tmp.%5$s)",
                  this.getTableName(), config.idColumnName(), config.timestampColumnName(), config.dataColumnName(),
                  config.segmentColumnName());
         }
      }
      return upsertRowSql;
   }

   @Override
   public void prepareUpsertStatement(PreparedStatement ps, String key, long timestamp, int segment, ByteBuffer byteBuffer) throws SQLException {
      ps.setString(1, key);
      ps.setLong(2, timestamp);
      // We must use BLOB here to avoid ORA-01461 caused by implicit casts on dual
      ps.setBlob(3, new ByteArrayInputStream(byteBuffer.getBuf(), byteBuffer.getOffset(), byteBuffer.getLength()), byteBuffer.getLength());
      if (!metaData.isSegmentedDisabled()) {
         ps.setInt(4, segment);
      }
   }

   @Override
   public void prepareUpdateStatement(PreparedStatement ps, String key, long timestamp, int segment, ByteBuffer byteBuffer) throws SQLException {
      ps.setLong(1, timestamp);
      ps.setBinaryStream(2, new ByteArrayInputStream(byteBuffer.getBuf(), byteBuffer.getOffset(), byteBuffer.getLength()), byteBuffer.getLength());
      ps.setString(3, key);
      if (!metaData.isSegmentedDisabled()) {
         ps.setInt(4, segment);
      }
   }
}
