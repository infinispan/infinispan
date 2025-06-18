package org.infinispan.persistence.jdbc.impl.table;

import static org.infinispan.persistence.jdbc.common.JdbcUtil.marshall;
import static org.infinispan.persistence.jdbc.common.JdbcUtil.unmarshall;
import static org.infinispan.persistence.jdbc.common.logging.Log.PERSISTENCE;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.function.Predicate;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.jdbc.common.JdbcUtil;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.persistence.jdbc.common.sql.BaseTableOperations;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.impl.PersistenceContextInitializerImpl;
import org.infinispan.persistence.keymappers.Key2StringMapper;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.UnsupportedKeyTypeException;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.MarshalledValue;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 */
public abstract class AbstractTableManager<K, V> extends BaseTableOperations<K, V> implements TableManager<K, V> {

   private static final String DEFAULT_IDENTIFIER_QUOTE_STRING = "\"";
   private static final String META_TABLE_SUFFIX = "_META";
   private static final String META_TABLE_DATA_COLUMN = "data";

   private final Log log;
   protected final InitializationContext ctx;
   protected final ConnectionFactory connectionFactory;
   protected final JdbcStringBasedStoreConfiguration jdbcConfig;
   protected final TableManipulationConfiguration config;
   protected final PersistenceMarshaller marshaller;
   protected final MarshallableEntryFactory<K, V> marshallableEntryFactory;
   protected final String timestampIndexExt = "timestamp_index";
   protected final String segmentIndexExt = "segment_index";

   protected final String identifierQuoteString;
   protected final DbMetaData dbMetadata;
   protected final TableName dataTableName;
   protected final TableName metaTableName;
   protected MetadataImpl metadata;
   protected Key2StringMapper key2StringMapper;

   // the field order is important because we are reusing some sql
   private final String insertRowSql;
   private final String updateRowSql;
   private final String upsertRowSql;
   private final String selectRowSql;
   private final String selectIdRowSql;
   private final String deleteRowSql;
   private final String getDeleteRowWithExpirationSql;
   private final String loadAllRowsSql;
   private final String countRowsSql;
   private final String loadAllNonExpiredRowsSql;
   private final String deleteAllRows;
   private final String selectExpiredRowsSql;

   AbstractTableManager(InitializationContext ctx, ConnectionFactory connectionFactory, JdbcStringBasedStoreConfiguration jdbcConfig,
         DbMetaData dbMetadata, String cacheName, Log log) {
      this(ctx, connectionFactory, jdbcConfig, dbMetadata, cacheName, DEFAULT_IDENTIFIER_QUOTE_STRING, log);
   }

   AbstractTableManager(InitializationContext ctx, ConnectionFactory connectionFactory, JdbcStringBasedStoreConfiguration jdbcConfig,
         DbMetaData dbMetadata, String cacheName, String identifierQuoteString, Log log) {
      super(jdbcConfig);
      // cacheName is required
      if (cacheName == null || cacheName.trim().isEmpty())
         throw new PersistenceException("cacheName needed in order to create table");

      this.ctx = ctx;
      this.connectionFactory = connectionFactory;
      this.jdbcConfig = jdbcConfig;
      this.config = jdbcConfig.table();
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
      this.getDeleteRowWithExpirationSql = initDeleteRowWithExpirationSql();
      this.loadAllRowsSql = initLoadAllRowsSql();
      this.countRowsSql = initCountNonExpiredRowsSql();
      this.loadAllNonExpiredRowsSql = initLoadNonExpiredAllRowsSql();
      this.deleteAllRows = initDeleteAllRowsSql();
      this.selectExpiredRowsSql = initSelectOnlyExpiredRowsSql();

      // ISPN-14108 only initiate variables from InitializationContext if not null. Required for StoreMigrator
      if (ctx != null) {
         this.marshaller = ctx.getPersistenceMarshaller();
         this.marshallableEntryFactory = ctx.getMarshallableEntryFactory();
         this.marshaller.register(new PersistenceContextInitializerImpl());
      } else {
         this.marshaller = null;
         this.marshallableEntryFactory = null;
      }
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

      JdbcStringBasedStoreConfiguration configuration = ctx.getConfiguration();
      try {
         Object mapper = Util.loadClassStrict(configuration.key2StringMapper(),
               ctx.getGlobalConfiguration().classLoader()).getDeclaredConstructor().newInstance();
         if (mapper instanceof Key2StringMapper) key2StringMapper = (Key2StringMapper) mapper;
      } catch (Exception e) {
         log.errorf("Trying to instantiate %s, however it failed due to %s", configuration.key2StringMapper(),
               e.getClass().getName());
         throw new IllegalStateException("This should not happen.", e);
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

   @Override
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
      String clearTable = "DELETE FROM " + metaTableName;
      executeUpdateSql(conn, clearTable);

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
   public Metadata getMetadata(Connection connection) throws PersistenceException {
      if (metadata == null) {
         ResultSet rs = null;
         try {
            String sql = String.format("SELECT %s FROM %s", META_TABLE_DATA_COLUMN, metaTableName.toString());
            rs = connection.createStatement().executeQuery(sql);
            if (!rs.next()) {
               log.sqlMetadataNotPresent(metaTableName.toString());
               return null;
            }
            this.metadata = unmarshall(rs.getBinaryStream(1), ctx.getPersistenceMarshaller());
         } catch (SQLException e) {
            PERSISTENCE.sqlFailureMetaRetrieval(e);
            throw new PersistenceException(e);
         } finally {
            JdbcUtil.safeClose(rs);
         }
      }
      return metadata;
   }

   @Override
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

      boolean indexExists = indexExists(getIndexName(dbMetadata.getMaxTableNameLength(), false, indexExt), conn);
      if (!indexExists) {
         String ddl = String.format("CREATE INDEX %s ON %s (%s)", getIndexName(dbMetadata.getMaxTableNameLength(), true, indexExt), dataTableName, columnName);
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

   @Override
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
      if (!indexExists(getIndexName(dbMetadata.getMaxTableNameLength(), true, indexName), conn)) return;

      String dropIndexDdl = getDropTimestampSql(indexName);
      if (log.isTraceEnabled()) {
         log.tracef("Dropping timestamp index with '%s'", dropIndexDdl);
      }
      executeUpdateSql(conn, dropIndexDdl);
   }

   protected String getDropTimestampSql(String indexName) {
      return String.format("DROP INDEX %s ON %s", getIndexName(dbMetadata.getMaxTableNameLength(), true, indexName), dataTableName);
   }

   @Override
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

   @Override
   public String getIdentifierQuoteString() {
      return identifierQuoteString;
   }

   @Override
   public TableName getDataTableName() {
      return dataTableName;
   }

   @Override
   public TableName getMetaTableName() {
      return metaTableName;
   }

   public String getIndexName(int maxTableNameLength, boolean withIdentifier, String indexExt) {
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
            config.dataColumnName(), config.idColumnName(), dataTableName, config.idColumnName());
   }

   @Override
   public String getSelectRowSql() {
      return selectRowSql;
   }

   protected String initSelectIdRowSql() {
      return String.format("SELECT %s FROM %s WHERE %s = ?", config.idColumnName(), dataTableName, config.idColumnName());
   }

   protected String initCountNonExpiredRowsSql() {
      return "SELECT COUNT(*) FROM " + dataTableName +
            " WHERE " + config.timestampColumnName() + " < 0 OR " + config.timestampColumnName() + " > ?";
   }

   protected String initDeleteRowSql() {
      return String.format("DELETE FROM %s WHERE %s = ?", dataTableName, config.idColumnName());
   }

   protected String initDeleteRowWithExpirationSql() {
      return String.format("DELETE FROM %s WHERE %s = ? AND %s = ?", dataTableName, config.idColumnName(), config.timestampColumnName());
   }

   @Override
   public String getDeleteRowSql() {
      return deleteRowSql;
   }

   @Override
   public String getDeleteRowWithExpirationSql() {
      return getDeleteRowWithExpirationSql;
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
      return String.format("SELECT %s, %s FROM %s", config.dataColumnName(), config.idColumnName(), dataTableName);
   }

   @Override
   public String getLoadAllRowsSql() {
      return loadAllRowsSql;
   }

   protected String initDeleteAllRowsSql() {
      return "DELETE FROM " + dataTableName;
   }

   protected String initSelectOnlyExpiredRowsSql() {
      return String.format("SELECT %1$s, %2$s, %3$s FROM %4$s WHERE %3$s < ? AND %3$s > 0", config.dataColumnName(),
            config.idColumnName(), config.timestampColumnName(), dataTableName);
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
   public boolean isStringEncodingRequired() {
      return false;
   }

   @Override
   public String encodeString(String string) {
      return string;
   }

   @Override
   public void prepareUpdateStatement(PreparedStatement ps, String key, long timestamp, int segment, ByteBuffer byteBuffer) throws SQLException {
      ps.setBinaryStream(1, new ByteArrayInputStream(byteBuffer.getBuf(), byteBuffer.getOffset(), byteBuffer.getLength()), byteBuffer.getLength());
      ps.setLong(2, timestamp);
      ps.setString(3, key);
   }

   @Override
   protected void preparePublishStatement(PreparedStatement ps, IntSet segments) throws SQLException {
      int offset = 1;
      ps.setLong(offset, ctx.getTimeService().wallClockTime());
      if (!dbMetadata.isSegmentedDisabled() && segments != null) {
         for (PrimitiveIterator.OfInt segIter = segments.iterator(); segIter.hasNext(); ) {
            ps.setInt(++offset, segIter.nextInt());
         }
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.JDBC_PERSISTED_METADATA)
   public static class MetadataImpl implements Metadata {
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

   protected String key2Str(Object key) throws PersistenceException {
      if (!key2StringMapper.isSupportedType(key.getClass())) {
         throw new UnsupportedKeyTypeException(key);
      }
      String keyStr = key2StringMapper.getStringMapping(key);
      return isStringEncodingRequired() ? encodeString(keyStr) : keyStr;
   }

   @Override
   public String getSelectAllSql(IntSet segments) {
      if (!dbMetadata.isSegmentedDisabled() && segments != null) {
         return getLoadNonExpiredRowsSqlForSegments(segments.size());
      } else {
         return getLoadNonExpiredAllRowsSql();
      }
   }

   @Override
   protected void prepareKeyStatement(PreparedStatement ps, Object key) throws SQLException {
      String lockingKey = key2Str(key);
      ps.setString(1, lockingKey);
   }

   @Override
   protected MarshallableEntry<K, V> entryFromResultSet(ResultSet rs, Object keyIfPresent, boolean fetchValue,
         Predicate<? super K> keyPredicate) throws SQLException {
      MarshallableEntry<K, V> entry = null;
      K key = (K) keyIfPresent;
      if (key == null) {
         String keyStr = rs.getString(2);
         key = (K) ((TwoWayKey2StringMapper) key2StringMapper).getKeyMapping(keyStr);
      }
      if (keyPredicate == null || keyPredicate.test(key)) {
         InputStream inputStream = rs.getBinaryStream(1);
         MarshalledValue value = unmarshall(inputStream, marshaller);
         entry = marshallableEntryFactory.create(key,
               fetchValue ? value.getValueBytes() : null,
               value.getMetadataBytes(),
               value.getInternalMetadataBytes(),
               value.getCreated(),
               value.getLastUsed());
         if (entry.getMetadata() != null && entry.isExpired(ctx.getTimeService().wallClockTime())) {
            return null;
         }
      }
      return entry;
   }

   @Override
   public String getDeleteAllSql() {
      return deleteAllRows;
   }

   @Override
   public String getUpsertRowSql() {
      return upsertRowSql;
   }

   @Override
   public String getSizeSql() {
      return countRowsSql;
   }

   @Override
   public void upsertEntry(Connection connection, int segment, MarshallableEntry<? extends K, ? extends V> entry)
         throws SQLException {
      if (!dbMetadata.isUpsertDisabled()) {
         super.upsertEntry(connection, segment, entry);
         return;
      }

      String keyStr = key2Str(entry.getKey());
      String sql = selectIdRowSql;
      if (log.isTraceEnabled()) {
         log.tracef("Running legacy upsert sql '%s'. Key string is '%s'", sql, keyStr);
      }
      PreparedStatement ps = null;
      try {
         ps = connection.prepareStatement(sql);
         ps.setQueryTimeout(configuration.readQueryTimeout());
         ps.setString(1, keyStr);
         ResultSet rs = ps.executeQuery();
         boolean update = rs.next();
         if (update) {
            sql = updateRowSql;
         } else {
            sql = insertRowSql;
         }
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         if (log.isTraceEnabled()) {
            log.tracef("Running sql '%s'. Key string is '%s'", sql, keyStr);
         }
         ps = connection.prepareStatement(sql);
         ps.setQueryTimeout(configuration.writeQueryTimeout());
         prepareValueStatement(ps, segment, entry);
         ps.executeUpdate();
      } finally {
         JdbcUtil.safeClose(ps);
      }
   }

   @Override
   protected final void prepareValueStatement(PreparedStatement ps, int segment, MarshallableEntry<? extends K, ? extends V> entry) throws SQLException {
      prepareValueStatement(ps, segment, key2Str(entry.getKey()), marshall(entry.getMarshalledValue(), marshaller), entry.expiryTime());
   }

   protected void prepareValueStatement(PreparedStatement ps, int segment, String keyStr, ByteBuffer valueBytes, long expiryTime) throws SQLException {
      ps.setBinaryStream(1, new ByteArrayInputStream(valueBytes.getBuf(), valueBytes.getOffset(),
            valueBytes.getLength()), valueBytes.getLength());
      ps.setLong(2, expiryTime);
      ps.setString(3, keyStr);
      if (!dbMetadata.isSegmentedDisabled()) {
         ps.setInt(4, segment);
      }
   }

   @Override
   protected void prepareSizeStatement(PreparedStatement ps) throws SQLException {
      ps.setLong(1, ctx.getTimeService().wallClockTime());
   }
}
