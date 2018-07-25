package org.infinispan.persistence.jdbc.table.management;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.spi.PersistenceException;

/**
 * @author Ryan Emerson
 */
public abstract class AbstractTableManager implements TableManager {

   private final Log log;
   protected final ConnectionFactory connectionFactory;
   protected final TableManipulationConfiguration config;
   protected final String timestampIndexExt = "timestamp_index";
   protected final String segmentIndexExt = "segment_index";

   protected String identifierQuoteString = "\"";
   protected String cacheName;
   protected DbMetaData metaData;
   protected TableName tableName;

   protected String insertRowSql;
   protected String updateRowSql;
   protected String upsertRowSql;
   protected String selectRowSql;
   protected String selectMultipleRowSql;
   protected String selectIdRowSql;
   protected String deleteRowSql;
   protected String loadAllRowsSql;
   protected String countRowsSql;
   protected String loadAllNonExpiredRowsSql;
   protected String deleteAllRows;
   protected String selectExpiredRowsSql;
   protected String deleteExpiredRowsSql;

   AbstractTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData, Log log) {
      this.connectionFactory = connectionFactory;
      this.config = config;
      this.metaData = metaData;
      this.log = log;
   }

   @Override
   public void start() throws PersistenceException {
      if (config.createOnStart()) {
         Connection conn = null;
         try {
            conn = connectionFactory.getConnection();
            if (!tableExists(conn)) {
               createTable(conn);
            }
            createIndex(conn, timestampIndexExt, config.timestampColumnName());
            if (!metaData.isSegmentedDisabled()) {
               createIndex(conn, segmentIndexExt, config.segmentColumnName());
            }
         } finally {
            connectionFactory.releaseConnection(conn);
         }
      }
   }

   @Override
   public void stop() throws PersistenceException {
      if (config.dropOnExit()) {
         Connection conn = null;
         try {
            conn = connectionFactory.getConnection();
            dropTable(conn);
         } finally {
            connectionFactory.releaseConnection(conn);
         }
      }
   }

   @Override
   public void setCacheName(String cacheName) {
      this.cacheName = cacheName;
      tableName = null;
   }

   public boolean tableExists(Connection connection) throws PersistenceException {
      return tableExists(connection, getTableName());
   }

   public boolean tableExists(Connection connection, TableName tableName) throws PersistenceException {
      Objects.requireNonNull(tableName, "table name is mandatory");
      ResultSet rs = null;
      try {
         // we need to make sure, that (even if the user has extended permissions) only the tables in current schema are checked
         // explicit set of the schema to the current user one to make sure only tables of the current users are requested
         DatabaseMetaData metaData = connection.getMetaData();
         String schemaPattern = tableName.getSchema();
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

   public void createTable(Connection conn) throws PersistenceException {
      if (cacheName == null || cacheName.trim().length() == 0)
         throw new PersistenceException("cacheName needed in order to create table");

      String ddl;
      if (metaData.isSegmentedDisabled()) {
         ddl = String.format("CREATE TABLE %1$s (%2$s %3$s NOT NULL, %4$s %5$s NOT NULL, %6$s %7$s NOT NULL, PRIMARY KEY (%2$s))",
               getTableName(), config.idColumnName(), config.idColumnType(), config.dataColumnName(),
               config.dataColumnType(), config.timestampColumnName(), config.timestampColumnType());
      } else {
         ddl = String.format("CREATE TABLE %1$s (%2$s %3$s NOT NULL, %4$s %5$s NOT NULL, %6$s %7$s NOT NULL, %8$s %9$s NOT NULL, PRIMARY KEY (%2$s))",
               getTableName(), config.idColumnName(), config.idColumnType(), config.dataColumnName(),
               config.dataColumnType(), config.timestampColumnName(), config.timestampColumnType(),
               config.segmentColumnName(), config.segmentColumnType());
      }

      if (log.isTraceEnabled()) {
         log.tracef("Creating table with following DDL: '%s'.", ddl);
      }
      executeUpdateSql(conn, ddl);
   }

   private void createIndex(Connection conn, String indexExt, String columnName) throws PersistenceException {
      if (metaData.isIndexingDisabled()) return;

      boolean indexExists = indexExists(getIndexName(false, indexExt), conn);
      if (!indexExists) {
         String ddl = String.format("CREATE INDEX %s ON %s (%s)", getIndexName(true, indexExt), getTableName(), columnName);
         if (log.isTraceEnabled()) {
            log.tracef("Adding index with following DDL: '%s'.", ddl);
         }
         executeUpdateSql(conn, ddl);
      }
   }

   protected boolean indexExists(String indexName, Connection conn) throws PersistenceException {
      ResultSet rs = null;
      try {
         TableName table = getTableName();
         DatabaseMetaData meta = conn.getMetaData();
         rs = meta.getIndexInfo(null, table.getSchema(), table.getName(), false, false);

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

   public void executeUpdateSql(Connection conn, String sql) throws PersistenceException {
      Statement statement = null;
      try {
         statement = conn.createStatement();
         statement.executeUpdate(sql);
      } catch (SQLException e) {
         log.errorCreatingTable(sql, e);
         throw new PersistenceException(e);
      } finally {
         JdbcUtil.safeClose(statement);
      }
   }

   public void dropTable(Connection conn) throws PersistenceException {
      dropIndex(conn, timestampIndexExt);
      dropIndex(conn, segmentIndexExt);

      String clearTable = "DELETE FROM " + getTableName();
      executeUpdateSql(conn, clearTable);

      String dropTableDdl = "DROP TABLE " + getTableName();
      if (log.isTraceEnabled()) {
         log.tracef("Dropping table with following DDL '%s'", dropTableDdl);
      }
      executeUpdateSql(conn, dropTableDdl);
   }

   protected void dropIndex(Connection conn, String indexName) throws PersistenceException {
      if (!indexExists(getIndexName(true, indexName), conn)) return;

      String dropIndexDdl = getDropTimestampSql(indexName);
      if (log.isTraceEnabled()) {
         log.tracef("Dropping timestamp index with '%s'", dropIndexDdl);
      }
      executeUpdateSql(conn, dropIndexDdl);
   }

   protected String getDropTimestampSql(String indexName) {
      return String.format("DROP INDEX %s ON %s", getIndexName(true, indexName), getTableName());
   }

   public int getFetchSize() {
      return config.fetchSize();
   }

   public int getBatchSize() {
      return config.batchSize();
   }

   @Override
   public boolean isUpsertSupported() {
      return !metaData.isUpsertDisabled();
   }

   public String getIdentifierQuoteString() {
      return identifierQuoteString;
   }

   public TableName getTableName() {
      if (tableName == null) {
         tableName = new TableName(identifierQuoteString, config.tableNamePrefix(), cacheName);
      }
      return tableName;
   }

   public String getIndexName(boolean withIdentifier, String indexExt) {
      TableName table = getTableName();
      String tableName = table.toString().replace(identifierQuoteString, "");
      String indexName = tableName + "_" + indexExt;
      if (withIdentifier) {
         return identifierQuoteString + indexName + identifierQuoteString;
      }
      return indexName;
   }

   @Override
   public String getInsertRowSql() {
      if (insertRowSql == null) {
         if (metaData.isSegmentedDisabled()) {
            insertRowSql = String.format("INSERT INTO %s (%s,%s,%s) VALUES (?,?,?)", getTableName(),
                  config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
         } else {
            insertRowSql = String.format("INSERT INTO %s (%s,%s,%s,%s) VALUES (?,?,?,?)", getTableName(),
                  config.dataColumnName(), config.timestampColumnName(), config.idColumnName(), config.segmentColumnName());
         }
      }
      return insertRowSql;
   }

   @Override
   public String getUpdateRowSql() {
      if (updateRowSql == null) {
         updateRowSql = String.format("UPDATE %s SET %s = ? , %s = ? WHERE %s = ?", getTableName(),
               config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      }
      return updateRowSql;
   }

   @Override
   public String getSelectRowSql() {
      if (selectRowSql == null) {
         selectRowSql = String.format("SELECT %s, %s FROM %s WHERE %s = ?",
                                      config.idColumnName(), config.dataColumnName(), getTableName(), config.idColumnName());
      }
      return selectRowSql;
   }

   protected String getSelectMultipleRowSql(int numberOfParams, String selectCriteria) {
      if (numberOfParams < 1)
         return null;

      if (numberOfParams == 1)
         return getSelectRowSql();

      StringBuilder sb = new StringBuilder(getSelectRowSql());
      for (int i = 0; i < numberOfParams - 1; i++) {
         sb.append(" OR ");
         sb.append(selectCriteria);
      }
      return sb.toString();
   }

   @Override
   public String getSelectMultipleRowSql(int numberOfParams) {
      return getSelectMultipleRowSql(numberOfParams, config.idColumnName() + " = ?");
   }

   @Override
   public String getSelectIdRowSql() {
      if (selectIdRowSql == null) {
         selectIdRowSql = String.format("SELECT %s FROM %s WHERE %s = ?", config.idColumnName(), getTableName(), config.idColumnName());
      }
      return selectIdRowSql;
   }

   @Override
   public String getCountNonExpiredRowsSql() {
      if (countRowsSql == null) {
         countRowsSql = "SELECT COUNT(*) FROM " + getTableName() +
         " WHERE " + config.timestampColumnName() + " < 0 OR " + config.timestampColumnName() + " > ?";
      }
      return countRowsSql;
   }

   @Override
   public String getCountNonExpiredRowsSqlForSegments(int numSegments) {
      StringBuilder stringBuilder = new StringBuilder("SELECT COUNT(*) FROM ");
      stringBuilder.append(getTableName());
      // Note the timestamp or is surrounded with parenthesis
      stringBuilder.append(" WHERE (");
      stringBuilder.append(config.timestampColumnName());
      stringBuilder.append(" > ? OR ");
      stringBuilder.append(config.timestampColumnName());
      stringBuilder.append(" < 0) AND ");
      stringBuilder.append(config.segmentColumnName());
      stringBuilder.append(" IN (?");

      for (int i = 1; i < numSegments; ++i) {
         stringBuilder.append(",?");
      }
      stringBuilder.append(")");

      return stringBuilder.toString();
   }

   @Override
   public String getDeleteRowSql() {
      if (deleteRowSql == null) {
         deleteRowSql = String.format("DELETE FROM %s WHERE %s = ?", getTableName(), config.idColumnName());
      }
      return deleteRowSql;
   }

   @Override
   public String getDeleteRowsSqlForSegments(int numSegments) {
      StringBuilder stringBuilder = new StringBuilder("DELETE FROM ");
      stringBuilder.append(getTableName());
      // Note the timestamp or is surrounded with parenthesis
      stringBuilder.append(" WHERE ");
      stringBuilder.append(config.segmentColumnName());
      stringBuilder.append(" IN (?");

      for (int i = 1; i < numSegments; ++i) {
         stringBuilder.append(",?");
      }
      stringBuilder.append(")");

      return stringBuilder.toString();
   }

   @Override
   public String getLoadNonExpiredAllRowsSql() {
      if (loadAllNonExpiredRowsSql == null) {
         loadAllNonExpiredRowsSql = String.format("SELECT %1$s, %2$s, %3$s FROM %4$s WHERE %3$s > ? OR %3$s < 0",
                                                  config.dataColumnName(), config.idColumnName(),
                                                  config.timestampColumnName(), getTableName());
      }
      return loadAllNonExpiredRowsSql;
   }

   @Override
   public String getLoadNonExpiredRowsSqlForSegments(int numSegments) {
      StringBuilder stringBuilder = new StringBuilder("SELECT ");
      stringBuilder.append(config.dataColumnName());
      stringBuilder.append(", ");
      stringBuilder.append(config.idColumnName());
      stringBuilder.append(" FROM ");
      stringBuilder.append(getTableName());
      // Note the timestamp or is surrounded with parenthesis
      stringBuilder.append(" WHERE (");
      stringBuilder.append(config.timestampColumnName());
      stringBuilder.append(" > ? OR ");
      stringBuilder.append(config.timestampColumnName());
      stringBuilder.append(" < 0) AND ");
      stringBuilder.append(config.segmentColumnName());
      stringBuilder.append(" IN (?");

      for (int i = 1; i < numSegments; ++i) {
         stringBuilder.append(",?");
      }
      stringBuilder.append(")");

      return stringBuilder.toString();
   }

   @Override
   public String getLoadAllRowsSql() {
      if (loadAllRowsSql == null) {
         loadAllRowsSql = String.format("SELECT %s, %s FROM %s", config.dataColumnName(),
                                        config.idColumnName(), getTableName());
      }
      return loadAllRowsSql;
   }

   @Override
   public String getDeleteAllRowsSql() {
      if (deleteAllRows == null) {
         deleteAllRows = "DELETE FROM " + getTableName();
      }
      return deleteAllRows;
   }

   @Override
   public String getSelectExpiredBucketsSql() {
      if (selectExpiredRowsSql == null) {
         selectExpiredRowsSql = String.format("%s WHERE %s < ?", getLoadAllRowsSql(), config.timestampColumnName());
      }
      return selectExpiredRowsSql;
   }

   @Override
   public String getSelectOnlyExpiredRowsSql() {
      if (deleteExpiredRowsSql == null) {
         deleteExpiredRowsSql = String.format("%1$s WHERE %2$s < ? AND %2$s > 0", getLoadAllRowsSql(), config.timestampColumnName());
      }
      return deleteExpiredRowsSql;
   }

   @Override
   public String getUpsertRowSql() {
      if (upsertRowSql == null) {
         if (metaData.isSegmentedDisabled()) {
            upsertRowSql = String.format("MERGE INTO %1$s " +
                        "USING (VALUES (?, ?, ?)) AS tmp (%2$s, %3$s, %4$s) " +
                        "ON (%2$s = tmp.%2$s) " +
                        "WHEN MATCHED THEN UPDATE SET %3$s = tmp.%3$s, %4$s = tmp.%4$s " +
                        "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s)",
                  getTableName(), config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
         } else {
            upsertRowSql = String.format("MERGE INTO %1$s " +
                        "USING (VALUES (?, ?, ?, ?)) AS tmp (%2$s, %3$s, %4$s, %5$s) " +
                        "ON (%2$s = tmp.%2$s) " +
                        "WHEN MATCHED THEN UPDATE SET %3$s = tmp.%3$s, %4$s = tmp.%4$s, %5$s = tmp.%5$s " +
                        "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s, %5$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s, tmp.%5$s)",
                  getTableName(), config.dataColumnName(), config.timestampColumnName(), config.idColumnName(), config.segmentColumnName());
         }

      }
      return upsertRowSql;
   }

   @Override
   public boolean isStringEncodingRequired() {
      return false;
   }

   @Override
   public String encodeString(String string) {
      return string;
   }

   @Override
   public void prepareUpsertStatement(PreparedStatement ps, String key, long timestamp, int segment, ByteBuffer byteBuffer) throws SQLException {
      ps.setBinaryStream(1, new ByteArrayInputStream(byteBuffer.getBuf(), byteBuffer.getOffset(), byteBuffer.getLength()), byteBuffer.getLength());
      ps.setLong(2, timestamp);
      ps.setString(3, key);
      if (!metaData.isSegmentedDisabled()) {
         ps.setInt(4, segment);
      }
   }

   @Override
   public void prepareUpdateStatement(PreparedStatement ps, String key, long timestamp, int segment, ByteBuffer byteBuffer) throws SQLException {
      ps.setBinaryStream(1, new ByteArrayInputStream(byteBuffer.getBuf(), byteBuffer.getOffset(), byteBuffer.getLength()), byteBuffer.getLength());
      ps.setLong(2, timestamp);
      ps.setString(3, key);
   }
}
