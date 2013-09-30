package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.TableManipulation;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.keymappers.Key2StringMapper;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.UnsupportedKeyTypeException;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.LogFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import static org.infinispan.persistence.PersistenceUtil.getExpiryTime;

/**
 * {@link org.infinispan.persistence.spi.AdvancedCacheLoader} implementation that stores the entries in a database. In contrast to the
 * {@link org.infinispan.persistence.jdbc.binary.JdbcBinaryStore}, this cache store will store each entry within a row
 * in the table (rather than grouping multiple entries into an row). This assures a finer grained granularity for all
 * operation, and better performance. In order to be able to store non-string keys, it relies on an {@link
 * org.infinispan.persistence.keymappers.Key2StringMapper}.
 * <p/>
 * Note that only the keys are stored as strings, the values are still saved as binary data. Using a character
 * data type for the value column will result in unmarshalling errors.
 * <p/>
 * The actual storage table is defined through configuration {@link org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration}. The table can
 * be
 * created/dropped on-the-fly, at deployment time. For more details consult javadoc for {@link
 * org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration}.
 * <p/>
 * It is recommended to use {@link JdbcStringBasedStore}} over
 * {@link org.infinispan.persistence.jdbc.binary.JdbcBinaryStore}} whenever it is possible, as is has a better performance.
 * One scenario in which this is not possible to use it though, is when you can't write an {@link org.infinispan.persistence.keymappers.Key2StringMapper}} to map the
 * keys to to string objects (e.g. when you don't have control over the types of the keys, for whatever reason).
 * <p/>
 * <b>Preload</b>.In order to support preload functionality the store needs to read the string keys from the database and transform them
 * into the corresponding key objects. {@link org.infinispan.persistence.keymappers.Key2StringMapper} only supports
 * key to string transformation(one way); in order to be able to use preload one needs to specify an
 * {@link org.infinispan.persistence.keymappers.TwoWayKey2StringMapper}, which extends {@link org.infinispan.persistence.keymappers.Key2StringMapper} and
 * allows bidirectional transformation.
 * <p/>
 * <b>Rehashing</b>. When a node leaves/joins, Infinispan moves around persistent state as part of rehashing process.
 * For this it needs access to the underlaying key objects, so if distribution is used, the mapper needs to be an
 * {@link org.infinispan.persistence.keymappers.TwoWayKey2StringMapper} otherwise the cache won't start (same constraint as with preloading).
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.persistence.keymappers.Key2StringMapper
 * @see org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper
 */
public class JdbcStringBasedStore implements AdvancedLoadWriteStore {

   private static final Log log = LogFactory.getLog(JdbcStringBasedStore.class, Log.class);

   private JdbcStringBasedStoreConfiguration configuration;

   private Key2StringMapper key2StringMapper;
   private ConnectionFactory connectionFactory;
   private TableManipulation tableManipulation;
   private InitializationContext ctx;
   private String cacheName;


   @Override
   public void init(InitializationContext ctx) {
      this.configuration = ctx.getConfiguration();
      this.ctx = ctx;
      cacheName = ctx.getCache().getName();
   }

   @Override
   public void start() {
      if (configuration.manageConnectionFactory()) {
         ConnectionFactory factory = ConnectionFactory.getConnectionFactory(configuration.connectionFactory().connectionFactoryClass());
         factory.start(configuration.connectionFactory(), factory.getClass().getClassLoader());
         initializeConnectionFactory(factory);
      }
      try {
         Object mapper = Class.forName(configuration.key2StringMapper()).newInstance();
         if (mapper instanceof Key2StringMapper) key2StringMapper = (Key2StringMapper) mapper;
      } catch (Exception e) {
         log.errorf("Trying to instantiate %s, however it failed due to %s", configuration.key2StringMapper(),
               e.getClass().getName());
         throw new IllegalStateException("This should not happen.", e);
      }
      if (log.isTraceEnabled()) {
         log.tracef("Using key2StringMapper: %s", key2StringMapper.getClass().getName());
      }
      if (configuration.preload()) {
         enforceTwoWayMapper("preload");
      }
      if (isDistributed()) {
         enforceTwoWayMapper("distribution/rehashing");
      }
   }

   @Override
   public void stop() {
      Throwable cause = null;
      try {
         tableManipulation.stop();
      } catch (Throwable t) {
         cause = t.getCause();
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }

      try {
         if (configuration.connectionFactory() instanceof ManagedConnectionFactory) {
            log.tracef("Stopping mananged connection factory: %s", connectionFactory);
            connectionFactory.stop();
         }
      } catch (Throwable t) {
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }
      if (cause != null) {
         throw new CacheLoaderException("Exceptions occurred while stopping store", cause);
      }
   }

   @Override
   public void write(MarshalledEntry entry) {
      Connection connection = null;
      PreparedStatement ps = null;
      String keyStr = key2Str(entry.getKey());
      try {
         connection = connectionFactory.getConnection();
         String sql = tableManipulation.getSelectIdRowSql();
         if (log.isTraceEnabled()) {
            log.tracef("Running sql '%s'. Key string is '%s'", sql, keyStr);
         }
         ps = connection.prepareStatement(sql);
         ps.setString(1, keyStr);
         ResultSet rs = ps.executeQuery();
         if (rs.next()) {
            sql = tableManipulation.getUpdateRowSql();
         } else {
            sql = tableManipulation.getInsertRowSql();
         }
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         if (log.isTraceEnabled()) {
             log.tracef("Running sql '%s'. Key string is '%s'", sql, keyStr);
         }
         ps = connection.prepareStatement(sql);
         updateStatement(entry, keyStr, ps);
         ps.executeUpdate();
      } catch (SQLException ex) {
         log.sqlFailureStoringKey(keyStr, ex);
         throw new CacheLoaderException(String.format("Error while storing string key to database; key: '%s'", keyStr), ex);
      } catch (InterruptedException e) {
         if (log.isTraceEnabled()) {
            log.trace("Interrupted while marshalling to store");
         }
         Thread.currentThread().interrupt();
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(connection);
      }
   }

   @Override
   public MarshalledEntry load(Object key) {
      String lockingKey = key2Str(key);
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      MarshalledEntry storedValue = null;
      try {
         String sql = tableManipulation.getSelectRowSql();
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ps.setString(1, lockingKey);
         rs = ps.executeQuery();
         if (rs.next()) {
            InputStream inputStream = rs.getBinaryStream(2);
            KeyValuePair<ByteBuffer, ByteBuffer> icv = JdbcUtil.unmarshall(ctx.getMarshaller(), inputStream);
            storedValue = ctx.getMarshalledEntryFactory().newMarshalledEntry(key, icv.getKey(), icv.getValue());
         }
      } catch (SQLException e) {
         log.sqlFailureReadingKey(key, lockingKey, e);
         throw new CacheLoaderException(String.format(
               "SQL error while fetching stored entry with key: %s, lockingKey: %s",
               key, lockingKey), e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
      if (storedValue != null && storedValue.getMetadata() != null &&
            storedValue.getMetadata().isExpired(ctx.getTimeService().wallClockTime())) {
         return null;
      }
      return storedValue;
   }

   @Override
   public boolean delete(Object key) {
      Connection connection = null;
      PreparedStatement ps = null;
      String keyStr = key2Str(key);
      try {
         String sql = tableManipulation.getDeleteRowSql();
         if (log.isTraceEnabled()) {
            log.tracef("Running sql '%s' on %s", sql, keyStr);
         }
         connection = connectionFactory.getConnection();
         ps = connection.prepareStatement(sql);
         ps.setString(1, keyStr);
         return ps.executeUpdate() == 1;
      } catch (SQLException ex) {
         log.sqlFailureRemovingKeys(ex);
         throw new CacheLoaderException("Error while removing string keys from database", ex);
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(connection);
      }
   }

   @Override
   public void clear() throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManipulation.getDeleteAllRowsSql();
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         int result = ps.executeUpdate();
         if (log.isTraceEnabled()) {
            log.tracef("Successfully removed %d rows.", result);
         }
      } catch (SQLException ex) {
         log.failedClearingJdbcCacheStore(ex);
         throw new CacheLoaderException("Failed clearing cache store", ex);
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   @Override
   public void purge(Executor executor, PurgeListener task) {
      //todo we should make the notification to the purge listener here
      ExecutorCompletionService<Void> ecs = new ExecutorCompletionService<Void>(executor);
      Future<Void> future = ecs.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            Connection conn = null;
            PreparedStatement ps = null;
            try {
               String sql = tableManipulation.getDeleteExpiredRowsSql();
               conn = connectionFactory.getConnection();
               ps = conn.prepareStatement(sql);
               ps.setLong(1, ctx.getTimeService().wallClockTime());
               int result = ps.executeUpdate();
               if (log.isTraceEnabled()) {
                  log.tracef("Successfully purged %d rows.", result);
               }
            } catch (SQLException ex) {
               log.failedClearingJdbcCacheStore(ex);
               throw new CacheLoaderException("Failed clearing string based JDBC store", ex);
            } finally {
               JdbcUtil.safeClose(ps);
               connectionFactory.releaseConnection(conn);
            }
            return null;
         }
      });
      try {
         future.get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
         log.errorExecutingParallelStoreTask(e);
         throw new CacheLoaderException(e);
      }
   }

   @Override
   public boolean contains(Object key) {
      //we can do better if needed...
      return load(key) != null;
   }


   @Override
   public void process(final KeyFilter filter, final CacheLoaderTask task, Executor executor, final boolean fetchValue, final boolean fetchMetadata) {

      ExecutorCompletionService<Void> ecs = new ExecutorCompletionService<Void>(executor);
      Future<Void> future = ecs.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
               String sql = tableManipulation.getLoadNonExpiredAllRowsSql();
               if (log.isTraceEnabled()) {
                  log.tracef("Running sql %s", sql);
               }
               conn = connectionFactory.getConnection();
               ps = conn.prepareStatement(sql);
               ps.setLong(1, ctx.getTimeService().wallClockTime());
               rs = ps.executeQuery();
               rs.setFetchSize(tableManipulation.getFetchSize());

               TaskContext taskContext = new TaskContextImpl();
               while (rs.next()) {
                  String keyStr = rs.getString(2);
                  Object key = ((TwoWayKey2StringMapper) key2StringMapper).getKeyMapping(keyStr);
                  if (taskContext.isStopped()) break;
                  if (filter != null && !filter.shouldLoadKey(key))
                     continue;
                  InputStream inputStream = rs.getBinaryStream(1);
                  MarshalledEntry entry;
                  if (fetchValue || fetchMetadata) {
                     KeyValuePair<ByteBuffer, ByteBuffer> kvp = JdbcUtil.unmarshall(ctx.getMarshaller(), inputStream);
                     entry = ctx.getMarshalledEntryFactory().newMarshalledEntry(key, kvp.getKey(), kvp.getValue());
                  } else {
                     entry = ctx.getMarshalledEntryFactory().newMarshalledEntry(key, (Object)null, null);
                  }
                  task.processEntry(entry, taskContext);
               }
               return null;
            } catch (SQLException e) {
               log.sqlFailureFetchingAllStoredEntries(e);
               throw new CacheLoaderException("SQL error while fetching all StoredEntries", e);
            } finally {
               JdbcUtil.safeClose(rs);
               JdbcUtil.safeClose(ps);
               connectionFactory.releaseConnection(conn);
            }
         }
      });
      try {
         future.get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
         log.errorExecutingParallelStoreTask(e);
         throw new CacheLoaderException(e);
      }
   }

   @Override
   public int size() {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         conn = connectionFactory.getConnection();
         String sql = tableManipulation.getCountRowsSql();
         ps = conn.prepareStatement(sql);
         rs = ps.executeQuery();
         rs.next();
         return rs.getInt(1);
      } catch (SQLException e) {
         log.sqlFailureIntegratingState(e);
         throw new CacheLoaderException("SQL failure while integrating state into store", e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   private void updateStatement(MarshalledEntry entry, String key, PreparedStatement ps) throws InterruptedException, SQLException {
      ByteBuffer byteBuffer = JdbcUtil.marshall(ctx.getMarshaller(), new KeyValuePair(entry.getValueBytes(), entry.getMetadataBytes()));
      ps.setBinaryStream(1, byteBuffer.getStream(), byteBuffer.getLength());
      ps.setLong(2, getExpiryTime(entry.getMetadata()));
      ps.setString(3, key);
   }

   private String key2Str(Object key) throws CacheLoaderException {
      if (!key2StringMapper.isSupportedType(key.getClass())) {
         throw new UnsupportedKeyTypeException(key);
      }
      return key2StringMapper.getStringMapping(key);
   }

   public boolean supportsKey(Class<?> keyType) {
      return key2StringMapper.isSupportedType(keyType);
   }

   /**
    * Keeps a reference to the connection factory for further use. Also initializes the {@link
    * org.infinispan.persistence.jdbc.TableManipulation} that needs connections. This method should be called when you don't
    * want the store to manage the connection factory, perhaps because it is using an shared connection factory: see
    * {@link org.infinispan.persistence.jdbc.mixed.JdbcMixedStore} for such an example of this.
    */
   public void initializeConnectionFactory(ConnectionFactory connectionFactory) throws CacheLoaderException {
      this.connectionFactory = connectionFactory;
      tableManipulation = new TableManipulation(configuration.table());
      tableManipulation.setCacheName(cacheName);
      tableManipulation.start(connectionFactory);
   }

   public ConnectionFactory getConnectionFactory() {
      return connectionFactory;
   }

   public TableManipulation getTableManipulation() {
      return tableManipulation;
   }

   private void enforceTwoWayMapper(String where) throws CacheLoaderException {
      if (!(key2StringMapper instanceof TwoWayKey2StringMapper)) {
         log.invalidKey2StringMapper(where, key2StringMapper.getClass().getName());
         throw new CacheLoaderException(String.format("Invalid key to string mapper", key2StringMapper.getClass().getName()));
      }
   }
   public boolean isDistributed() {
      return ctx.getCache().getCacheConfiguration() != null && ctx.getCache().getCacheConfiguration().clustering().cacheMode().isDistributed();
   }

}
