package org.infinispan.persistence.jdbc.impl.table;

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

   private static final String DEFAULT_IDENTIFIER_QUOTE_STRING = "\"";

   private final Log log;
   protected final ConnectionFactory connectionFactory;
   protected final TableManipulationConfiguration config;
   protected final String timestampIndexExt = "timestamp_index";
   protected final String segmentIndexExt = "segment_index";

   protected final String identifierQuoteString;
   protected final DbMetaData metaData;
   protected final TableName tableName;

   // the field order is important because we are reusing some sql
   private final String insertRowSql;
   private final String updateRowSql;
   private final String upsertRowSql;
   private final String selectRowSql;
   private final String selectIdRowSql;
   private final String deleteRowSql;
   private final String loadAllRowsSql;
   private final String countRowsSql;
   private final String loadAllNonExpiredRowsSql;
   private final String deleteAllRows;
   private final String selectExpiredRowsSql;
   private final String deleteExpiredRowsSql;

   AbstractTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData, String cacheName, Log log) {
      this(connectionFactory, config, metaData, cacheName, DEFAULT_IDENTIFIER_QUOTE_STRING, log);
   }

   AbstractTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData, String cacheName, String identifierQuoteString, Log log) {
      // cacheName is required
      if (cacheName == null || cacheName.trim().length() == 0)
         throw new PersistenceException("cacheName needed in order to create table");

      this.connectionFactory = connectionFactory;
      this.config = config;
      this.metaData = metaData;
      this.tableName = new TableName(identifierQuoteString, config.tableNamePrefix(), cacheName);
      this.identifierQuoteString = identifierQuoteString;
      this.log = log;

      // init row sql
      this.insertRowSql = initInsertRowSql();
      this.updateRowSql = initUpdateRowSql();
      this.upsertRowSql = initUpsertRowSql();
      this.selectRowSql = initSelectRowSql();
      this.selectIdRowSql = initSelectIdRowSql();
      this.deleteRowSql = initDeleteRowSql();
      this.loadAllRowsSql = initLoadAllRowsSql();
      this.countRowsSql = initCountNonExpiredRowsSql();
      this.loadAllNonExpiredRowsSql = initLoadNonExpiredAllRowsSql();
      this.deleteAllRows = initDeleteAllRowsSql();
      this.selectExpiredRowsSql = initSelectExpiredBucketsSql();
      this.deleteExpiredRowsSql = initSelectOnlyExpiredRowsSql();
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

   public boolean tableExists(Connection connection) throws PersistenceException {
      return tableExists(connection, tableName);
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
      String ddl;
      if (metaData.isSegmentedDisabled()) {
         ddl = String.format("CREATE TABLE %1$s (%2$s %3$s NOT NULL, %4$s %5$s NOT NULL, %6$s %7$s NOT NULL, PRIMARY KEY (%2$s))",
               tableName, config.idColumnName(), config.idColumnType(), config.dataColumnName(),
               config.dataColumnType(), config.timestampColumnName(), config.timestampColumnType());
      } else {
         ddl = String.format("CREATE TABLE %1$s (%2$s %3$s NOT NULL, %4$s %5$s NOT NULL, %6$s %7$s NOT NULL, %8$s %9$s NOT NULL, PRIMARY KEY (%2$s))",
               tableName, config.idColumnName(), config.idColumnType(), config.dataColumnName(),
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
         String ddl = String.format("CREATE INDEX %s ON %s (%s)", getIndexName(true, indexExt), tableName, columnName);
         if (log.isTraceEnabled()) {
            log.tracef("Adding index with following DDL: '%s'.", ddl);
         }
         executeUpdateSql(conn, ddl);
      }
   }

   protected boolean indexExists(String indexName, Connection conn) throws PersistenceException {
      ResultSet rs = null;
      try {
         DatabaseMetaData meta = conn.getMetaData();
         rs = meta.getIndexInfo(null, tableName.getSchema(), tableName.getName(), false, false);

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

      String clearTable = "DELETE FROM " + tableName;
      executeUpdateSql(conn, clearTable);

      String dropTableDdl = "DROP TABLE " + tableName;
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
      return String.format("DROP INDEX %s ON %s", getIndexName(true, indexName), tableName);
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
      return tableName;
   }

   public String getIndexName(boolean withIdentifier, String indexExt) {
      String plainTableName = tableName.toString().replace(identifierQuoteString, "");
      String indexName = plainTableName + "_" + indexExt;
      if (withIdentifier) {
         return identifierQuoteString + indexName + identifierQuoteString;
      }
      return indexName;
   }

   protected String initInsertRowSql() {
      if (metaData.isSegmentedDisabled()) {
         return String.format("INSERT INTO %s (%s,%s,%s) VALUES (?,?,?)", tableName,
               config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      } else {
         return String.format("INSERT INTO %s (%s,%s,%s,%s) VALUES (?,?,?,?)", tableName,
               config.dataColumnName(), config.timestampColumnName(), config.idColumnName(), config.segmentColumnName());
      }
   }

   @Override
   public String getInsertRowSql() {
      return insertRowSql;
   }

   protected String initUpdateRowSql() {
      return String.format("UPDATE %s SET %s = ? , %s = ? WHERE %s = ?", tableName,
            config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
   }

   @Override
   public String getUpdateRowSql() {
      return updateRowSql;
   }

   protected String initSelectRowSql() {
      return String.format("SELECT %s, %s FROM %s WHERE %s = ?",
            config.idColumnName(), config.dataColumnName(), tableName, config.idColumnName());
   }

   @Override
   public String getSelectRowSql() {
      return selectRowSql;
   }

   protected String initSelectIdRowSql() {
      return String.format("SELECT %s FROM %s WHERE %s = ?", config.idColumnName(), tableName, config.idColumnName());
   }

   @Override
   public String getSelectIdRowSql() {
      return selectIdRowSql;
   }

   protected String initCountNonExpiredRowsSql() {
      return "SELECT COUNT(*) FROM " + tableName +
            " WHERE " + config.timestampColumnName() + " < 0 OR " + config.timestampColumnName() + " > ?";
   }

   @Override
   public String getCountNonExpiredRowsSql() {
      return countRowsSql;
   }

   @Override
   public String getCountNonExpiredRowsSqlForSegments(int numSegments) {
      StringBuilder stringBuilder = new StringBuilder("SELECT COUNT(*) FROM ");
      stringBuilder.append(tableName);
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

   protected String initDeleteRowSql() {
      return String.format("DELETE FROM %s WHERE %s = ?", tableName, config.idColumnName());
   }

   @Override
   public String getDeleteRowSql() {
      return deleteRowSql;
   }

   @Override
   public String getDeleteRowsSqlForSegments(int numSegments) {
      StringBuilder stringBuilder = new StringBuilder("DELETE FROM ");
      stringBuilder.append(tableName);
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

   protected String initLoadNonExpiredAllRowsSql() {
      return String.format("SELECT %1$s, %2$s, %3$s FROM %4$s WHERE %3$s > ? OR %3$s < 0",
            config.dataColumnName(), config.idColumnName(),
            config.timestampColumnName(), tableName);
   }

   @Override
   public String getLoadNonExpiredAllRowsSql() {
      return loadAllNonExpiredRowsSql;
   }

   @Override
   public String getLoadNonExpiredRowsSqlForSegments(int numSegments) {
      StringBuilder stringBuilder = new StringBuilder("SELECT ");
      stringBuilder.append(config.dataColumnName());
      stringBuilder.append(", ");
      stringBuilder.append(config.idColumnName());
      stringBuilder.append(" FROM ");
      stringBuilder.append(tableName);
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

   protected String initLoadAllRowsSql() {
      return String.format("SELECT %s, %s FROM %s", config.dataColumnName(),
            config.idColumnName(), tableName);
   }

   @Override
   public String getLoadAllRowsSql() {
      return loadAllRowsSql;
   }

   protected String initDeleteAllRowsSql() {
      return "DELETE FROM " + tableName;
   }

   @Override
   public String getDeleteAllRowsSql() {
      return deleteAllRows;
   }

   protected String initSelectExpiredBucketsSql() {
      return String.format("%s WHERE %s < ?", loadAllRowsSql, config.timestampColumnName());
   }

   protected String initSelectOnlyExpiredRowsSql() {
      return String.format("%1$s WHERE %2$s < ? AND %2$s > 0", getLoadAllRowsSql(), config.timestampColumnName());
   }

   @Override
   public String getSelectOnlyExpiredRowsSql() {
      return deleteExpiredRowsSql;
   }

   protected String initUpsertRowSql() {
      if (metaData.isSegmentedDisabled()) {
         return String.format("MERGE INTO %1$s " +
                     "USING (VALUES (?, ?, ?)) AS tmp (%2$s, %3$s, %4$s) " +
                     "ON (%2$s = tmp.%2$s) " +
                     "WHEN MATCHED THEN UPDATE SET %3$s = tmp.%3$s, %4$s = tmp.%4$s " +
                     "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s)",
               tableName, config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      } else {
         return String.format("MERGE INTO %1$s " +
                     "USING (VALUES (?, ?, ?, ?)) AS tmp (%2$s, %3$s, %4$s, %5$s) " +
                     "ON (%2$s = tmp.%2$s) " +
                     "WHEN MATCHED THEN UPDATE SET %3$s = tmp.%3$s, %4$s = tmp.%4$s, %5$s = tmp.%5$s " +
                     "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s, %5$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s, tmp.%5$s)",
               tableName, config.dataColumnName(), config.timestampColumnName(), config.idColumnName(), config.segmentColumnName());
      }
   }

   @Override
   public String getUpsertRowSql() {
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
