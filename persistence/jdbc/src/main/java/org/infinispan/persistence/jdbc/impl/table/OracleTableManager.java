package org.infinispan.persistence.jdbc.impl.table;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import org.infinispan.commons.io.ByteBuffer;
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
class OracleTableManager extends AbstractTableManager {

   private static final Log log = LogFactory.getLog(OracleTableManager.class, Log.class);

   private static final String TIMESTAMP_INDEX_PREFIX = "IDX";
   private static final String SEGMENT_INDEX_PREFIX = "SDX";
   private final int dbVersion;

   OracleTableManager(InitializationContext ctx, ConnectionFactory connectionFactory, JdbcStringBasedStoreConfiguration config, DbMetaData metaData, String cacheName) {
      super(ctx, connectionFactory, config, metaData, cacheName, log);
      dbVersion = dbMetadata.getMajorVersion() * 100 + dbMetadata.getMinorVersion();
   }

   @Override
   public boolean tableExists(Connection connection, TableName tableName) {
      Objects.requireNonNull(tableName, "table name is mandatory");
      ResultSet rs = null;
      try {
         DatabaseMetaData metaData = connection.getMetaData();
         String schemaPattern = tableName.getSchema() == null ? metaData.getUserName() : tableName.getSchema();
         rs = metaData.getTables(null, schemaPattern, tableName.getName(), new String[]{"TABLE"});
         return rs.next();
      } catch (SQLException e) {
         if (log.isTraceEnabled())
            log.tracef(e, "SQLException occurs while checking the table %s", tableName);
         return false;
      } finally {
         JdbcUtil.safeClose(rs);
      }
   }

   @Override
   protected boolean indexExists(String indexName, Connection conn) throws PersistenceException {
      ResultSet rs = null;
      try {
         DatabaseMetaData metaData = conn.getMetaData();
         String schemaPattern = dataTableName.getSchema() == null ? metaData.getUserName() : dataTableName.getSchema();
         rs = metaData.getIndexInfo(null,
                 String.format("%1$s%2$s%1$s", identifierQuoteString, schemaPattern),
                 String.format("%1$s%2$s%1$s", identifierQuoteString, dataTableName.getName()),
                 false, false);
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
   public String getIndexName(int maxTableNameLength, boolean withIdentifier, String indexExt) {
      if (indexExt.equals(timestampIndexExt)) {
         // Timestamp for Oracle began with IDX, to keep backwards compatible we have to keep using that
         indexExt = TIMESTAMP_INDEX_PREFIX;
      }
      String plainTableName = dataTableName.getName();
      /*  Oracle version 12.1 and below supports index names only 30 characters long.
          If cache names have length greater that 15 and have the same prefix it is possible to have the same index names timestamp and segments.
      */
      if (dbVersion <= 1201 && indexExt.equals(segmentIndexExt) && plainTableName.length() + indexExt.length() + 1 > maxTableNameLength) {
         indexExt = SEGMENT_INDEX_PREFIX;
      }
      int maxNameSize = maxTableNameLength - indexExt.length() - 1;
      String truncatedName = plainTableName.length() > maxNameSize ? plainTableName.substring(0, maxNameSize) : plainTableName;
      String indexName = indexExt + "_" + truncatedName;
      if (withIdentifier) {
         return identifierQuoteString + indexName + identifierQuoteString;
      }
      return indexName;
   }

   protected String getDropTimestampSql() {
      return String.format("DROP INDEX %s", getIndexName(-1, true, timestampIndexExt));
   }

   @Override
   protected String initInsertRowSql() {
      if (dbMetadata.isSegmentedDisabled()) {
         return String.format("INSERT INTO %s (%s,%s,%s) VALUES (?,?,?)", dataTableName,
               config.idColumnName(), config.timestampColumnName(), config.dataColumnName());
      } else {
         return String.format("INSERT INTO %s (%s,%s,%s,%s) VALUES (?,?,?,?)", dataTableName,
               config.idColumnName(), config.timestampColumnName(), config.dataColumnName(), config.segmentColumnName());
      }
   }

   @Override
   protected String initUpdateRowSql() {
      return String.format("UPDATE %s SET %s = ? , %s = ? WHERE %s = ?", dataTableName,
            config.timestampColumnName(), config.dataColumnName(), config.idColumnName());
   }

   @Override
   public String initUpsertRowSql() {
      if (dbMetadata.isSegmentedDisabled()) {
         return String.format("MERGE INTO %1$s t " +
                     "USING (SELECT ? %2$s, ? %3$s, ? %4$s from dual) tmp ON (t.%2$s = tmp.%2$s) " +
                     "WHEN MATCHED THEN UPDATE SET t.%3$s = tmp.%3$s, t.%4$s = tmp.%4$s " +
                     "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s)",
               dataTableName, config.idColumnName(), config.timestampColumnName(), config.dataColumnName());
      } else {
         return String.format("MERGE INTO %1$s t " +
                     "USING (SELECT ? %2$s, ? %3$s, ? %4$s, ? %5$s from dual) tmp ON (t.%2$s = tmp.%2$s) " +
                     "WHEN MATCHED THEN UPDATE SET t.%3$s = tmp.%3$s, t.%4$s = tmp.%4$s " +
                     "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s, %5$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s, tmp.%5$s)",
               dataTableName, config.idColumnName(), config.timestampColumnName(), config.dataColumnName(),
               config.segmentColumnName());
      }
   }

   @Override
   protected void prepareValueStatement(PreparedStatement ps, int segment, String keyStr, ByteBuffer valueBytes, long expiryTime) throws SQLException {
      ps.setString(1, keyStr);
      ps.setLong(2, expiryTime);
      // We must use BLOB here to avoid ORA-01461 caused by implicit casts on dual
      ps.setBlob(3, new ByteArrayInputStream(valueBytes.getBuf(), valueBytes.getOffset(), valueBytes.getLength()), valueBytes.getLength());
      if (!dbMetadata.isSegmentedDisabled()) {
         ps.setInt(4, segment);
      }
   }

   @Override
   public void prepareUpdateStatement(PreparedStatement ps, String key, long timestamp, int segment, ByteBuffer byteBuffer) throws SQLException {
      ps.setLong(1, timestamp);
      ps.setBinaryStream(2, new ByteArrayInputStream(byteBuffer.getBuf(), byteBuffer.getOffset(), byteBuffer.getLength()), byteBuffer.getLength());
      ps.setString(3, key);
      if (!dbMetadata.isSegmentedDisabled()) {
         ps.setInt(4, segment);
      }
   }
}
