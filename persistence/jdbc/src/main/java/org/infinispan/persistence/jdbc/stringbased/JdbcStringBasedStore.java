package org.infinispan.persistence.jdbc.stringbased;

import static org.infinispan.persistence.PersistenceUtil.getExpiryTime;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.transaction.Transaction;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.jdbc.table.management.TableManager;
import org.infinispan.persistence.jdbc.table.management.TableManagerFactory;
import org.infinispan.persistence.keymappers.Key2StringMapper;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.UnsupportedKeyTypeException;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;
import org.infinispan.persistence.spi.TransactionalCacheWriter;
import org.infinispan.persistence.support.BatchModification;
import org.infinispan.util.KeyValuePair;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * {@link org.infinispan.persistence.spi.AdvancedCacheLoader} implementation that stores the entries in a database.
 * This cache store will store each entry within a row in the table. This assures a finer grained granularity for all
 * operation, and better performance. In order to be able to store non-string keys, it relies on an {@link
 * org.infinispan.persistence.keymappers.Key2StringMapper}.
 * <p>
 * Note that only the keys are stored as strings, the values are still saved as binary data. Using a character
 * data type for the value column will result in unmarshalling errors.
 * <p>
 * The actual storage table is defined through configuration {@link org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration}.
 * The table can be created/dropped on-the-fly, at deployment time. For more details consult javadoc for {@link
 * org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration}.
 * <p>
 * <b>Preload</b>.In order to support preload functionality the store needs to read the string keys from the database and transform them
 * into the corresponding key objects. {@link org.infinispan.persistence.keymappers.Key2StringMapper} only supports
 * key to string transformation(one way); in order to be able to use preload one needs to specify an
 * {@link org.infinispan.persistence.keymappers.TwoWayKey2StringMapper}, which extends {@link org.infinispan.persistence.keymappers.Key2StringMapper} and
 * allows bidirectional transformation.
 * <p>
 * <b>Rehashing</b>. When a node leaves/joins, Infinispan moves around persistent state as part of rehashing process.
 * For this it needs access to the underlaying key objects, so if distribution is used, the mapper needs to be an
 * {@link org.infinispan.persistence.keymappers.TwoWayKey2StringMapper} otherwise the cache won't start (same constraint as with preloading).
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.persistence.keymappers.Key2StringMapper
 * @see org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper
 */
@Store(shared = true)
@ConfiguredBy(JdbcStringBasedStoreConfiguration.class)
public class JdbcStringBasedStore<K,V> implements SegmentedAdvancedLoadWriteStore<K,V>, TransactionalCacheWriter<K,V> {

   private static final Log log = LogFactory.getLog(JdbcStringBasedStore.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Map<Transaction, Connection> transactionConnectionMap = new ConcurrentHashMap<>();
   private JdbcStringBasedStoreConfiguration configuration;

   private GlobalConfiguration globalConfiguration;
   private Key2StringMapper key2StringMapper;
   private String cacheName;
   private ConnectionFactory connectionFactory;
   private MarshalledEntryFactory<K, V> marshalledEntryFactory;
   private StreamingMarshaller marshaller;
   private TableManager tableManager;
   private TimeService timeService;
   private KeyPartitioner keyPartitioner;
   private boolean isDistributedCache;

   @Override
   public void init(InitializationContext ctx) {
      this.configuration = ctx.getConfiguration();
      this.cacheName = ctx.getCache().getName();
      this.globalConfiguration = ctx.getCache().getCacheManager().getCacheManagerConfiguration();
      this.marshalledEntryFactory = ctx.getMarshalledEntryFactory();
      this.marshaller = ctx.getMarshaller();
      this.timeService = ctx.getTimeService();
      this.keyPartitioner = configuration.segmented() ? ctx.getKeyPartitioner() : null;
      this.isDistributedCache = ctx.getCache().getCacheConfiguration() != null && ctx.getCache().getCacheConfiguration().clustering().cacheMode().isDistributed();
   }

   @Override
   public void start() {
      if (configuration.manageConnectionFactory()) {
         ConnectionFactory factory = ConnectionFactory.getConnectionFactory(configuration.connectionFactory().connectionFactoryClass());
         factory.start(configuration.connectionFactory(), factory.getClass().getClassLoader());
         initializeConnectionFactory(factory);
      }

      try {
         Object mapper = Util.loadClassStrict(configuration.key2StringMapper(),
                                              globalConfiguration.classLoader()).newInstance();
         if (mapper instanceof Key2StringMapper) key2StringMapper = (Key2StringMapper) mapper;
      } catch (Exception e) {
         log.errorf("Trying to instantiate %s, however it failed due to %s", configuration.key2StringMapper(),
                    e.getClass().getName());
         throw new IllegalStateException("This should not happen.", e);
      }
      if (trace) {
         log.tracef("Using key2StringMapper: %s", key2StringMapper.getClass().getName());
      }
      if (configuration.preload()) {
         enforceTwoWayMapper("preload");
      }
      if (isDistributedCache) {
         enforceTwoWayMapper("distribution/rehashing");
      }
   }

   @Override
   public void stop() {
      Throwable cause = null;
      try {
         tableManager.stop();
         tableManager = null;
      } catch (Throwable t) {
         cause = t.getCause();
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }

      try {
         if (configuration.connectionFactory() instanceof ManagedConnectionFactory) {
            log.tracef("Stopping managed connection factory: %s", connectionFactory);
            connectionFactory.stop();
         }
      } catch (Throwable t) {
         if (cause == null) {
            cause = t;
         } else {
            t.addSuppressed(cause);
         }
         log.debug("Exception while stopping", t);
      }
      if (cause != null) {
         throw new PersistenceException("Exceptions occurred while stopping store", cause);
      }
   }

   @Override
   public boolean isAvailable() {
      if (tableManager == null || connectionFactory == null)
         return false;

      Connection connection = null;
      try {
         connection = connectionFactory.getConnection();
         return connection != null && connection.isValid(10);
      } catch (SQLException e) {
         return false;
      } finally {
         connectionFactory.releaseConnection(connection);
      }
   }

   void initializeConnectionFactory(ConnectionFactory connectionFactory) throws PersistenceException {
      this.connectionFactory = connectionFactory;
      tableManager = getTableManager();
      tableManager.setCacheName(cacheName);
      tableManager.start();
   }

   public ConnectionFactory getConnectionFactory() {
      return connectionFactory;
   }

   private int getSegment(MarshalledEntry entry) {
      if (keyPartitioner == null) {
         return -1;
      }
      return keyPartitioner.getSegment(entry.getKey());
   }

   @Override
   public void write(MarshalledEntry entry) {
      Connection connection = null;
      String keyStr = key2Str(entry.getKey());
      try {
         connection = connectionFactory.getConnection();
         write(entry, connection, keyStr, getSegment(entry));
      } catch (SQLException ex) {
         log.sqlFailureStoringKey(keyStr, ex);
         throw new PersistenceException(String.format("Error while storing string key to database; key: '%s'", keyStr), ex);
      } catch (InterruptedException e) {
         if (trace) {
            log.trace("Interrupted while marshalling to store");
         }
         Thread.currentThread().interrupt();
      } finally {
         connectionFactory.releaseConnection(connection);
      }
   }

   private void write(MarshalledEntry entry, Connection connection, int segment) throws SQLException, InterruptedException {
      write(entry, connection, key2Str(entry.getKey()), segment);
   }

   private void write(MarshalledEntry entry, Connection connection, String keyStr, int segment) throws SQLException, InterruptedException {
      if (tableManager.isUpsertSupported()) {
         executeUpsert(connection, entry, keyStr, segment);
      } else {
         executeLegacyUpdate(connection, entry, keyStr, segment);
      }
   }

   private void executeUpsert(Connection connection, MarshalledEntry entry, String keyStr, int segment)
         throws InterruptedException, SQLException {
      PreparedStatement ps = null;
      String sql = tableManager.getUpsertRowSql();
      if (trace) {
         log.tracef("Running sql '%s'. Key string is '%s'", sql, keyStr);
      } try {
         ps = connection.prepareStatement(sql);
         prepareUpsertStatement(entry, keyStr, segment, ps);
         ps.executeUpdate();
      } finally {
         JdbcUtil.safeClose(ps);
      }
   }

   private void executeLegacyUpdate(Connection connection, MarshalledEntry entry, String keyStr, int segment)
         throws InterruptedException, SQLException {
      String sql = tableManager.getSelectIdRowSql();
      if (trace) {
         log.tracef("Running sql '%s'. Key string is '%s'", sql, keyStr);
      }
      PreparedStatement ps = null;
      try {
         ps = connection.prepareStatement(sql);
         ps.setString(1, keyStr);
         ResultSet rs = ps.executeQuery();
         boolean update = rs.next();
         if (update) {
            sql = tableManager.getUpdateRowSql();
         } else {
            sql = tableManager.getInsertRowSql();
         }
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         if (trace) {
            log.tracef("Running sql '%s'. Key string is '%s'", sql, keyStr);
         }
         ps = connection.prepareStatement(sql);
         prepareStatement(entry, keyStr, segment, ps, !update);
         ps.executeUpdate();
      } finally {
         JdbcUtil.safeClose(ps);
      }
   }

   @Override
   public void writeBatch(Iterable<MarshalledEntry<? extends K, ? extends V>> marshalledEntries) {
      // If upsert is not supported, then we must execute the legacy write for each entry; i.e. read then update/insert
      if (!tableManager.isUpsertSupported()) {
         marshalledEntries.forEach(this::write);
         return;
      }

      Connection connection = null;
      try {
         connection = connectionFactory.getConnection();
         try (PreparedStatement upsertBatch = connection.prepareStatement(tableManager.getUpsertRowSql())) {
            int batchSize = 0;
            for (MarshalledEntry entry : marshalledEntries) {
               String keyStr = key2Str(entry.getKey());
               prepareUpsertStatement(entry, keyStr, getSegment(entry), upsertBatch);
               upsertBatch.addBatch();
               batchSize++;

               if (batchSize == configuration.maxBatchSize()) {
                  batchSize = 0;
                  upsertBatch.executeBatch();
                  upsertBatch.clearBatch();
               }
            }

            if (batchSize != 0)
               upsertBatch.executeBatch();
         }
      } catch (SQLException | InterruptedException e) {
         throw log.sqlFailureWritingBatch(e);
      } finally {
         connectionFactory.releaseConnection(connection);
      }
   }

   @Override
   public void deleteBatch(Iterable<Object> keys) {
      Connection connection = null;
      try {
         connection = connectionFactory.getConnection();
         try (PreparedStatement deleteBatch = connection.prepareStatement(tableManager.getDeleteRowSql())) {
            int batchSize = 0;
            for (Object key : keys) {
               String keyStr = key2Str(key);
               deleteBatch.setString(1, keyStr);
               deleteBatch.addBatch();
               batchSize++;

               if (batchSize == configuration.maxBatchSize()) {
                  batchSize = 0;
                  deleteBatch.executeBatch();
                  deleteBatch.clearBatch();
               }
            }

            if (batchSize != 0)
               deleteBatch.executeBatch();
         }
      } catch (SQLException e) {
         throw log.sqlFailureDeletingBatch(keys, e);
      } finally {
         connectionFactory.releaseConnection(connection);
      }
   }

   @Override
   public MarshalledEntry<K, V> load(Object key) {
      String lockingKey = key2Str(key);
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      MarshalledEntry<K, V> storedValue = null;
      try {
         String sql = tableManager.getSelectRowSql();
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ps.setString(1, lockingKey);
         rs = ps.executeQuery();
         if (rs.next()) {
            InputStream inputStream = rs.getBinaryStream(2);
            KeyValuePair<ByteBuffer, ByteBuffer> icv = unmarshall(inputStream);
            storedValue = marshalledEntryFactory.newMarshalledEntry(key, icv.getKey(), icv.getValue());
         }
      } catch (SQLException e) {
         log.sqlFailureReadingKey(key, lockingKey, e);
         throw new PersistenceException(String.format(
               "SQL error while fetching stored entry with key: %s, lockingKey: %s",
               key, lockingKey), e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
      if (storedValue != null && storedValue.getMetadata() != null &&
            storedValue.getMetadata().isExpired(timeService.wallClockTime())) {
         return null;
      }
      return storedValue;
   }

   @Override
   public void clear() {
      Connection conn = null;
      Statement statement = null;
      try {
         String sql = tableManager.getDeleteAllRowsSql();
         conn = connectionFactory.getConnection();
         statement = conn.createStatement();
         int result = statement.executeUpdate(sql);
         if (log.isTraceEnabled()) {
            log.tracef("Successfully removed %d rows.", result);
         }
      } catch (SQLException ex) {
         log.failedClearingJdbcCacheStore(ex);
         throw new PersistenceException("Failed clearing cache store", ex);
      } finally {
         JdbcUtil.safeClose(statement);
         connectionFactory.releaseConnection(conn);
      }
   }

   @Override
   public void clear(IntSet segments) {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManager.getDeleteRowsSqlForSegments(segments.size());
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         int offset = 0;
         for (PrimitiveIterator.OfInt segIter = segments.iterator(); segIter.hasNext(); ) {
            ps.setInt(++offset, segIter.nextInt());
         }
         int result = ps.executeUpdate();
         if (log.isTraceEnabled()) {
            log.tracef("Successfully removed %d rows.", result);
         }
      } catch (SQLException ex) {
         log.failedClearingJdbcCacheStore(ex);
         throw new PersistenceException("Failed clearing cache store when using segments " + segments, ex);
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   @Override
   public boolean delete(Object key) {
      Connection connection = null;
      PreparedStatement ps = null;
      String keyStr = key2Str(key);
      try {
         String sql = tableManager.getDeleteRowSql();
         if (trace) {
            log.tracef("Running sql '%s' on %s", sql, keyStr);
         }
         connection = connectionFactory.getConnection();
         ps = connection.prepareStatement(sql);
         ps.setString(1, keyStr);
         return ps.executeUpdate() == 1;
      } catch (SQLException ex) {
         log.sqlFailureRemovingKeys(ex);
         throw new PersistenceException("Error while removing string keys from database", ex);
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(connection);
      }
   }

   @Override
   public void purge(Executor executor, PurgeListener purgeListener) {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         String sql = tableManager.getSelectOnlyExpiredRowsSql();
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ps.setLong(1, timeService.wallClockTime());
         rs = ps.executeQuery();

         try (PreparedStatement batchDelete = conn.prepareStatement(tableManager.getDeleteRowSql())) {
            int affectedRows = 0;
            boolean twoWayMapperExists = key2StringMapper instanceof TwoWayKey2StringMapper;
            while (rs.next()) {
               affectedRows++;
               String keyStr = rs.getString(2);
               batchDelete.setString(1, keyStr);
               batchDelete.addBatch();

               if (twoWayMapperExists && purgeListener != null) {
                  Object key = ((TwoWayKey2StringMapper) key2StringMapper).getKeyMapping(keyStr);
                  purgeListener.entryPurged(key);
               }
            }

            if (!twoWayMapperExists)
               log.twoWayKey2StringMapperIsMissing(TwoWayKey2StringMapper.class.getSimpleName());

            if (affectedRows > 0) {
               int[] result = batchDelete.executeBatch();
               if (trace) {
                  log.tracef("Successfully purged %d rows.", result.length);
               }
            }
         }
      } catch (SQLException ex) {
         log.failedClearingJdbcCacheStore(ex);
         throw new PersistenceException("Failed clearing string based JDBC store", ex);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   @Override
   public boolean contains(Object key) {
      //we can do better if needed...
      return load(key) != null;
   }

   private <P> Flowable<P> publish(IntSet segments, Function<ResultSet, Flowable<P>> function) {
      return Flowable.using(() -> {
         Connection connection = connectionFactory.getConnection();
         String sql;
         if (segments != null) {
            sql = tableManager.getLoadNonExpiredRowsSqlForSegments(segments.size());
         } else {
            sql = tableManager.getLoadNonExpiredAllRowsSql();
         }
         if (trace) {
            log.tracef("Running sql %s", sql);
         }
         // Store both together so we can be sure they are both closed after the publisher is done
         return new KeyValuePair<>(connection, connection.prepareStatement(sql));
      }, kvp -> {
         PreparedStatement ps = kvp.getValue();
         int offset = 1;
         ps.setLong(offset, timeService.wallClockTime());
         if (segments != null) {
            for (PrimitiveIterator.OfInt segIter = segments.iterator(); segIter.hasNext(); ) {
               ps.setInt(++offset, segIter.nextInt());
            }
         }
         ps.setFetchSize(tableManager.getFetchSize());
         ResultSet rs = ps.executeQuery();
         return function.apply(rs);
      }, kvp -> {
         JdbcUtil.safeClose(kvp.getValue());
         connectionFactory.releaseConnection(kvp.getKey());
      });
   }

   @Override
   public Flowable<K> publishKeys(Predicate<? super K> filter) {
      return publish(null, rs -> Flowable.fromIterable(() -> new ResultSetKeyIterator(rs, filter)));
   }

   @Override
   public Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      return publish(segments, rs -> Flowable.fromIterable(() -> new ResultSetKeyIterator(rs, filter)));
   }

   @Override
   public Flowable<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      return publish(null, rs -> Flowable.fromIterable(() -> new ResultSetEntryIterator(rs, filter, fetchValue, fetchMetadata)));
   }

   @Override
   public Publisher<MarshalledEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      return publish(segments, rs -> Flowable.fromIterable(() -> new ResultSetEntryIterator(rs, filter, fetchValue, fetchMetadata)));
   }

   @Override
   public void prepareWithModifications(Transaction transaction, BatchModification batchModification) throws PersistenceException {
      try {
         Connection connection = getTxConnection(transaction);
         connection.setAutoCommit(false);

         boolean upsertSupported = tableManager.isUpsertSupported();
         try (PreparedStatement upsertBatch = upsertSupported ? connection.prepareStatement(tableManager.getUpsertRowSql()) : null;
              PreparedStatement deleteBatch = connection.prepareStatement(tableManager.getDeleteRowSql())) {

            for (MarshalledEntry entry : batchModification.getMarshalledEntries()) {
               int segment = getSegment(entry);
               if (upsertSupported) {
                  String keyStr = key2Str(entry.getKey());
                  prepareUpsertStatement(entry, keyStr, segment, upsertBatch);
                  upsertBatch.addBatch();
               } else {
                  write(entry, connection, segment);
               }
            }

            for (Object key : batchModification.getKeysToRemove()) {
               String keyStr = key2Str(key);
               deleteBatch.setString(1, keyStr);
               deleteBatch.addBatch();
            }

            if (upsertSupported && !batchModification.getMarshalledEntries().isEmpty())
               upsertBatch.executeBatch();

            if (!batchModification.getKeysToRemove().isEmpty())
               deleteBatch.executeUpdate();
         }
         // We do not call connection.close() in the event of an exception, as close() on active Tx behaviour is implementation
         // dependent. See https://docs.oracle.com/javase/8/docs/api/java/sql/Connection.html#close--
      } catch (SQLException | InterruptedException e) {
         throw log.prepareTxFailure(e);
      }
   }

   @Override
   public void commit(Transaction tx) {
      Connection connection;
      try {
         connection = getTxConnection(tx);
         connection.commit();
      } catch (SQLException e) {
         log.sqlFailureTxCommit(e);
         throw new PersistenceException(String.format("Error during commit of JDBC transaction (%s)", tx), e);
      } finally {
         destroyTxConnection(tx);
      }
   }

   @Override
   public void rollback(Transaction tx) {
      Connection connection;
      try {
         connection = getTxConnection(tx);
         connection.rollback();
      } catch (SQLException e) {
         log.sqlFailureTxRollback(e);
         throw new PersistenceException(String.format("Error during rollback of JDBC transaction (%s)", tx), e);
      } finally {
         destroyTxConnection(tx);
      }
   }

   private Connection getTxConnection(Transaction tx) {
      Connection connection = transactionConnectionMap.get(tx);
      if (connection == null) {
         connection = connectionFactory.getConnection();
         transactionConnectionMap.put(tx, connection);
      }
      return connection;
   }

   private void destroyTxConnection(Transaction tx) {
      Connection connection = transactionConnectionMap.remove(tx);
      if (connection != null)
         connectionFactory.releaseConnection(connection);
   }

   @Override
   public int size() {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         conn = connectionFactory.getConnection();
         String sql = tableManager.getCountNonExpiredRowsSql();
         ps = conn.prepareStatement(sql);
         ps.setLong(1, timeService.wallClockTime());
         rs = ps.executeQuery();
         rs.next();
         return rs.getInt(1);
      } catch (SQLException e) {
         log.sqlFailureIntegratingState(e);
         throw new PersistenceException("SQL failure while integrating state into store", e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   @Override
   public int size(IntSet segments) {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         conn = connectionFactory.getConnection();
         String sql = tableManager.getCountNonExpiredRowsSqlForSegments(segments.size());
         ps = conn.prepareStatement(sql);
         int offset = 1;
         ps.setLong(offset, timeService.wallClockTime());
         for (PrimitiveIterator.OfInt segIter = segments.iterator(); segIter.hasNext(); ) {
            ps.setInt(++offset, segIter.nextInt());
         }
         rs = ps.executeQuery();
         rs.next();
         return rs.getInt(1);
      } catch (SQLException e) {
         log.sqlFailureIntegratingState(e);
         throw new PersistenceException("SQL failure while integrating state into store", e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   private void prepareUpsertStatement(MarshalledEntry entry, String key, int segment, PreparedStatement ps) throws InterruptedException, SQLException {
      prepareStatement(entry, key, segment, ps, true);
   }

   private void prepareStatement(MarshalledEntry entry, String key, int segment, PreparedStatement ps, boolean upsert) throws InterruptedException, SQLException {
      ByteBuffer byteBuffer = marshall(new KeyValuePair(entry.getValueBytes(), entry.getMetadataBytes()));
      long expiryTime = getExpiryTime(entry.getMetadata());
      if (upsert) {
         tableManager.prepareUpsertStatement(ps, key, expiryTime, segment, byteBuffer);
      } else {
         tableManager.prepareUpdateStatement(ps, key, expiryTime, segment, byteBuffer);
      }
   }

   private String key2Str(Object key) throws PersistenceException {
      if (!key2StringMapper.isSupportedType(key.getClass())) {
         throw new UnsupportedKeyTypeException(key);
      }
      String keyStr = key2StringMapper.getStringMapping(key);
      return tableManager.isStringEncodingRequired() ? tableManager.encodeString(keyStr) : keyStr;
   }

   public TableManager getTableManager() {
      if (tableManager == null)
         tableManager = TableManagerFactory.getManager(connectionFactory, configuration);
      return tableManager;
   }

   private void enforceTwoWayMapper(String where) throws PersistenceException {
      if (!(key2StringMapper instanceof TwoWayKey2StringMapper)) {
         log.invalidKey2StringMapper(where, key2StringMapper.getClass().getName());
         throw new PersistenceException(String.format("Invalid key to string mapper : %s", key2StringMapper.getClass().getName()));
      }
   }

   private ByteBuffer marshall(Object obj) throws PersistenceException, InterruptedException {
      try {
         return marshaller.objectToBuffer(obj);
      } catch (IOException e) {
         log.errorMarshallingObject(e, obj);
         throw new PersistenceException("I/O failure while marshalling object: " + obj, e);
      }
   }

   @SuppressWarnings("unchecked")
   private <T> T unmarshall(InputStream inputStream) throws PersistenceException {
      try {
         return (T) marshaller.objectFromInputStream(inputStream);
      } catch (IOException e) {
         log.ioErrorUnmarshalling(e);
         throw new PersistenceException("I/O error while unmarshalling from stream", e);
      } catch (ClassNotFoundException e) {
         log.unexpectedClassNotFoundException(e);
         throw new PersistenceException("*UNEXPECTED* ClassNotFoundException. This should not happen as Bucket class exists", e);
      }
   }

   private class ResultSetEntryIterator extends AbstractIterator<MarshalledEntry<K, V>> {
      private final ResultSet rs;
      private final Predicate<? super K> filter;
      private final boolean fetchValue;
      private final boolean fetchMetadata;

      public ResultSetEntryIterator(ResultSet rs, Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
         this.rs = rs;
         this.filter = filter;
         this.fetchValue = fetchValue;
         this.fetchMetadata = fetchMetadata;
      }

      @Override
      protected MarshalledEntry<K, V> getNext() {
         MarshalledEntry<K, V> entry = null;
         try {
            while (entry == null && rs.next()) {
               String keyStr = rs.getString(2);
               K key = (K) ((TwoWayKey2StringMapper) key2StringMapper).getKeyMapping(keyStr);

               if (filter == null || filter.test(key)) {
                  if (fetchValue || fetchMetadata) {
                     InputStream inputStream = rs.getBinaryStream(1);
                     KeyValuePair<ByteBuffer, ByteBuffer> kvp = unmarshall(inputStream);
                     entry = marshalledEntryFactory.newMarshalledEntry(
                           key, fetchValue ? kvp.getKey() : null, fetchMetadata ? kvp.getValue() : null);
                  } else {
                     entry = marshalledEntryFactory.newMarshalledEntry(key, (Object) null, null);
                  }
               }
            }
         } catch (SQLException e) {
            throw new CacheException(e);
         }
         return entry;
      }
   }

   private class ResultSetKeyIterator extends AbstractIterator<K> {
      private final ResultSet rs;
      private final Predicate<? super K> filter;

      public ResultSetKeyIterator(ResultSet rs, Predicate<? super K> filter) {
         this.rs = rs;
         this.filter = filter;
      }

      @Override
      protected K getNext() {
         K key = null;
         try {
            while (key == null && rs.next()) {
               String keyStr = rs.getString(2);
               K testKey = (K) ((TwoWayKey2StringMapper) key2StringMapper).getKeyMapping(keyStr);
               if (filter == null || filter.test(testKey)) {
                  key = testKey;
               }
            }
         } catch (SQLException e) {
            throw new CacheException(e);
         }
         return key;
      }
   }
}
