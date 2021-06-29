package org.infinispan.persistence.jdbc.impl.table;

import static org.infinispan.persistence.jdbc.JdbcUtil.marshall;
import static org.infinispan.persistence.jdbc.JdbcUtil.unmarshall;
import static org.infinispan.persistence.jdbc.logging.Log.PERSISTENCE;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Version;
import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.PersistenceContextInitializerImpl;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 */
public abstract class AbstractTableManager implements TableManager {

   private static final String DEFAULT_IDENTIFIER_QUOTE_STRING = "\"";
   private static final String META_TABLE_SUFFIX = "_META";
   private static final String META_TABLE_DATA_COLUMN = "data";

   private final Log log;
   protected final InitializationContext ctx;
   protected final ConnectionFactory connectionFactory;
   protected final TableManipulationConfiguration config;
   protected final String timestampIndexExt = "timestamp_index";
   protected final String segmentIndexExt = "segment_index";

   protected final String identifierQuoteString;
   protected final DbMetaData dbMetadata;
   protected final TableName dataTableName;
   protected final TableName metaTableName;
   protected MetadataImpl metadata;

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

   AbstractTableManager(InitializationContext ctx, ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData dbMetadata, String cacheName, Log log) {
      this(ctx, connectionFactory, config, dbMetadata, cacheName, DEFAULT_IDENTIFIER_QUOTE_STRING, log);
   }

   AbstractTableManager(InitializationContext ctx, ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData dbMetadata, String cacheName, String identifierQuoteString, Log log) {
      // cacheName is required
      if (cacheName == null || cacheName.trim().length() == 0)
         throw new PersistenceException("cacheName needed in order to create table");

      this.ctx = ctx;
      this.connectionFactory = connectionFactory;
      this.config = config;
      this.dbMetadata = dbMetadata;
      this.dataTableName = new TableName(identifierQuoteString, config.tableNamePrefix(), cacheName);
      this.metaTableName = new TableName(identifierQuoteString, config.tableNamePrefix(), cacheName + META_TABLE_SUFFIX);
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
      this.selectExpiredRowsSql = initSelectOnlyExpiredRowsSql();

      ctx.getPersistenceMarshaller().register(new PersistenceContextInitializerImpl());
   }

   @Override
   public void start() throws PersistenceException {
      if (config.createOnStart()) {
         Connection conn = null;
         try {
            conn = connectionFactory.getConnection();
            if (!tableExists(conn, metaTableName)) {
               createMetaTable(conn);
            }

            if (!tableExists(conn, dataTableName)) {
               createDataTable(conn);
            }
            createIndex(conn, timestampIndexExt, config.timestampColumnName());
            if (!dbMetadata.isSegmentedDisabled()) {
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
            dropTables(conn);
         } finally {
            connectionFactory.releaseConnection(conn);
         }
      }
   }

   public boolean tableExists(Connection connection, TableName tableName) {
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

   @Override
   public void createMetaTable(Connection conn) throws PersistenceException {
      // Store using internal names for columns and store as binary using the provided dataColumnType so no additional configuration is required
      String sql = String.format("CREATE TABLE %1$s (%2$s %3$s NOT NULL)", metaTableName, META_TABLE_DATA_COLUMN, config.dataColumnType());
      executeUpdateSql(conn, sql);
      updateMetaTable(conn);
   }

   @Override
   public void updateMetaTable(Connection conn) throws PersistenceException {
      short version = Version.getVersionShort();
      int segments = ctx.getConfiguration().segmented() ? ctx.getCache().getCacheConfiguration().clustering().hash().numSegments() : -1;
      this.metadata = new MetadataImpl(version, segments);
      ByteBuffer buffer = marshall(metadata, ctx.getPersistenceMarshaller());

      String sql = String.format("INSERT INTO %s (%s) VALUES (?)", metaTableName, META_TABLE_DATA_COLUMN);
      PreparedStatement ps = null;
      try {
         ps = conn.prepareStatement(sql);
         ps.setBinaryStream(1, new ByteArrayInputStream(buffer.getBuf(), buffer.getOffset(), buffer.getLength()));
         ps.executeUpdate();
      } catch (SQLException e) {
         PERSISTENCE.errorCreatingTable(sql, e);
         throw new PersistenceException(e);
      } finally {
         JdbcUtil.safeClose(ps);
      }
   }

   @Override
   public TableManager.Metadata getMetadata(Connection connection) throws PersistenceException {
      if (metadata == null) {
         try {
            String sql = String.format("SELECT %s FROM %s", META_TABLE_DATA_COLUMN, metaTableName.toString());
            ResultSet rs = connection.createStatement().executeQuery(sql);
            rs.next();
            this.metadata = unmarshall(rs.getBinaryStream(1), ctx.getPersistenceMarshaller());
         } catch (SQLException e) {
            PERSISTENCE.sqlFailureMetaRetrieval(e);
            throw new PersistenceException(e);
         }
      }
      return metadata;
   }

   public void createDataTable(Connection conn) throws PersistenceException {
      String ddl;
      if (dbMetadata.isSegmentedDisabled()) {
         ddl = String.format("CREATE TABLE %1$s (%2$s %3$s NOT NULL, %4$s %5$s NOT NULL, %6$s %7$s NOT NULL, PRIMARY KEY (%2$s))",
               dataTableName, config.idColumnName(), config.idColumnType(), config.dataColumnName(),
               config.dataColumnType(), config.timestampColumnName(), config.timestampColumnType());
      } else {
         ddl = String.format("CREATE TABLE %1$s (%2$s %3$s NOT NULL, %4$s %5$s NOT NULL, %6$s %7$s NOT NULL, %8$s %9$s NOT NULL, PRIMARY KEY (%2$s))",
               dataTableName, config.idColumnName(), config.idColumnType(), config.dataColumnName(),
               config.dataColumnType(), config.timestampColumnName(), config.timestampColumnType(),
               config.segmentColumnName(), config.segmentColumnType());
      }

      if (log.isTraceEnabled()) {
         log.tracef("Creating table with following DDL: '%s'.", ddl);
      }
      executeUpdateSql(conn, ddl);
   }


   private void createIndex(Connection conn, String indexExt, String columnName) throws PersistenceException {
      if (dbMetadata.isIndexingDisabled()) return;

      boolean indexExists = indexExists(getIndexName(false, indexExt), conn);
      if (!indexExists) {
         String ddl = String.format("CREATE INDEX %s ON %s (%s)", getIndexName(true, indexExt), dataTableName, columnName);
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
         rs = meta.getIndexInfo(null, dataTableName.getSchema(), dataTableName.getName(), false, false);

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
         PERSISTENCE.errorCreatingTable(sql, e);
         throw new PersistenceException(e);
      } finally {
         JdbcUtil.safeClose(statement);
      }
   }

   public void dropDataTable(Connection conn) throws PersistenceException {
      dropIndex(conn, timestampIndexExt);
      dropIndex(conn, segmentIndexExt);
      dropTable(conn, dataTableName);
   }

   @Override
   public void dropMetaTable(Connection conn) throws PersistenceException {
      dropTable(conn, metaTableName);
   }

   private void dropTable(Connection conn, TableName tableName) throws PersistenceException {
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
      return String.format("DROP INDEX %s ON %s", getIndexName(true, indexName), dataTableName);
   }

   public int getFetchSize() {
      return config.fetchSize();
   }

   public int getBatchSize() {
      return config.batchSize();
   }

   @Override
   public boolean isUpsertSupported() {
      return !dbMetadata.isUpsertDisabled();
   }

   public String getIdentifierQuoteString() {
      return identifierQuoteString;
   }

   public TableName getDataTableName() {
      return dataTableName;
   }

   @Override
   public TableName getMetaTableName() {
      return metaTableName;
   }

   public String getIndexName(boolean withIdentifier, String indexExt) {
      String plainTableName = dataTableName.toString().replace(identifierQuoteString, "");
      String indexName = plainTableName + "_" + indexExt;
      if (withIdentifier) {
         return identifierQuoteString + indexName + identifierQuoteString;
      }
      return indexName;
   }

   protected String initInsertRowSql() {
      if (dbMetadata.isSegmentedDisabled()) {
         return String.format("INSERT INTO %s (%s,%s,%s) VALUES (?,?,?)", dataTableName,
               config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      } else {
         return String.format("INSERT INTO %s (%s,%s,%s,%s) VALUES (?,?,?,?)", dataTableName,
               config.dataColumnName(), config.timestampColumnName(), config.idColumnName(), config.segmentColumnName());
      }
   }

   @Override
   public String getInsertRowSql() {
      return insertRowSql;
   }

   protected String initUpdateRowSql() {
      return String.format("UPDATE %s SET %s = ? , %s = ? WHERE %s = ?", dataTableName,
            config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
   }

   @Override
   public String getUpdateRowSql() {
      return updateRowSql;
   }

   protected String initSelectRowSql() {
      return String.format("SELECT %s, %s FROM %s WHERE %s = ?",
            config.idColumnName(), config.dataColumnName(), dataTableName, config.idColumnName());
   }

   @Override
   public String getSelectRowSql() {
      return selectRowSql;
   }

   protected String initSelectIdRowSql() {
      return String.format("SELECT %s FROM %s WHERE %s = ?", config.idColumnName(), dataTableName, config.idColumnName());
   }

   @Override
   public String getSelectIdRowSql() {
      return selectIdRowSql;
   }

   protected String initCountNonExpiredRowsSql() {
      return "SELECT COUNT(*) FROM " + dataTableName +
            " WHERE " + config.timestampColumnName() + " < 0 OR " + config.timestampColumnName() + " > ?";
   }

   @Override
   public String getCountNonExpiredRowsSql() {
      return countRowsSql;
   }

   @Override
   public String getCountNonExpiredRowsSqlForSegments(int numSegments) {
      StringBuilder stringBuilder = new StringBuilder("SELECT COUNT(*) FROM ");
      stringBuilder.append(dataTableName);
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
      return String.format("DELETE FROM %s WHERE %s = ?", dataTableName, config.idColumnName());
   }

   @Override
   public String getDeleteRowSql() {
      return deleteRowSql;
   }

   @Override
   public String getDeleteRowsSqlForSegments(int numSegments) {
      StringBuilder stringBuilder = new StringBuilder("DELETE FROM ");
      stringBuilder.append(dataTableName);
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
      if (dbMetadata.isSegmentedDisabled()) {
         return String.format("SELECT %1$s, %2$s, %3$s FROM %4$s WHERE %3$s > ? OR %3$s < 0",
               config.dataColumnName(), config.idColumnName(),
               config.timestampColumnName(), dataTableName);
      } else {
         return String.format("SELECT %1$s, %2$s, %3$s, %4$s FROM %5$s WHERE %3$s > ? OR %3$s < 0",
               config.dataColumnName(), config.idColumnName(),
               config.timestampColumnName(), config.segmentColumnName(), dataTableName);
      }
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
      stringBuilder.append(dataTableName);
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
            config.idColumnName(), dataTableName);
   }

   @Override
   public String getLoadAllRowsSql() {
      return loadAllRowsSql;
   }

   protected String initDeleteAllRowsSql() {
      return "DELETE FROM " + dataTableName;
   }

   @Override
   public String getDeleteAllRowsSql() {
      return deleteAllRows;
   }

   protected String initSelectOnlyExpiredRowsSql() {
      return String.format("%1$s WHERE %2$s < ? AND %2$s > 0 FOR UPDATE", getLoadAllRowsSql(), config.timestampColumnName());
   }

   @Override
   public String getSelectOnlyExpiredRowsSql() {
      return selectExpiredRowsSql;
   }

   protected String initUpsertRowSql() {
      if (dbMetadata.isSegmentedDisabled()) {
         return String.format("MERGE INTO %1$s " +
                     "USING (VALUES (?, ?, ?)) AS tmp (%2$s, %3$s, %4$s) " +
                     "ON (%2$s = tmp.%2$s) " +
                     "WHEN MATCHED THEN UPDATE SET %3$s = tmp.%3$s, %4$s = tmp.%4$s " +
                     "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s)",
               dataTableName, config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      } else {
         return String.format("MERGE INTO %1$s " +
                     "USING (VALUES (?, ?, ?, ?)) AS tmp (%2$s, %3$s, %4$s, %5$s) " +
                     "ON (%2$s = tmp.%2$s) " +
                     "WHEN MATCHED THEN UPDATE SET %3$s = tmp.%3$s, %4$s = tmp.%4$s, %5$s = tmp.%5$s " +
                     "WHEN NOT MATCHED THEN INSERT (%2$s, %3$s, %4$s, %5$s) VALUES (tmp.%2$s, tmp.%3$s, tmp.%4$s, tmp.%5$s)",
               dataTableName, config.dataColumnName(), config.timestampColumnName(), config.idColumnName(), config.segmentColumnName());
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
      if (!dbMetadata.isSegmentedDisabled()) {
         ps.setInt(4, segment);
      }
   }

   @Override
   public void prepareUpdateStatement(PreparedStatement ps, String key, long timestamp, int segment, ByteBuffer byteBuffer) throws SQLException {
      ps.setBinaryStream(1, new ByteArrayInputStream(byteBuffer.getBuf(), byteBuffer.getOffset(), byteBuffer.getLength()), byteBuffer.getLength());
      ps.setLong(2, timestamp);
      ps.setString(3, key);
   }

   @ProtoTypeId(ProtoStreamTypeIds.JDBC_PERSISTED_METADATA)
   public static class MetadataImpl implements TableManager.Metadata {
      final short version;
      final int segments;

      @ProtoFactory
      public MetadataImpl(short version, int segments) {
         this.version = version;
         this.segments = segments;
      }

      @Override
      @ProtoField(number = 1, defaultValue = "-1")
      public short getVersion() {
         return version;
      }

      @Override
      @ProtoField(number = 2, defaultValue = "-1")
      public int getSegments() {
         return segments;
      }

      @Override
      public String toString() {
         return "MetadataImpl{" +
               "version=" + version +
               ", segments=" + segments +
               '}';
      }
   }
}
