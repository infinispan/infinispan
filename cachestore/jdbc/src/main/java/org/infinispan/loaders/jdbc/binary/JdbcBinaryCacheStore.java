package org.infinispan.loaders.jdbc.binary;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.loaders.bucket.BucketBasedCacheStore;
import org.infinispan.loaders.jdbc.DataManipulationHelper;
import org.infinispan.loaders.jdbc.JdbcUtil;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.configuration.JdbcBinaryCacheStoreConfiguration;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.loaders.jdbc.logging.Log;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * {@link BucketBasedCacheStore} implementation that will store all the buckets as rows in database, each row
 * corresponding to a bucket. This is in contrast to {@link org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore}
 * which stores each StoredEntry as a row in the database.
 * </p>
 * It is generally recommended to use {@link org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore} whenever
 * possible as it performs better. Please read {@link org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore}'s
 * javadoc for more details on this.
 * <p/>
 * This class has the benefit of being able to store StoredEntries that do not have String keys, at the cost of coarser
 * grained access granularity, and inherently performance.
 * <p/>
 * All the DB related configurations are described in {@link org.infinispan.loaders.jdbc.binary
 * .JdbcBinaryCacheStoreConfiguration}.
 *
 * @author Mircea.Markus@jboss.com
 * @see JdbcBinaryCacheStoreConfiguration
 * @see org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore
 */
public class JdbcBinaryCacheStore extends BucketBasedCacheStore {

   private static final Log log = LogFactory.getLog(JdbcBinaryCacheStore.class, Log.class);

   private final static byte BINARY_STREAM_DELIMITER = 100;

   private JdbcBinaryCacheStoreConfiguration configuration;

   private ConnectionFactory connectionFactory;
   TableManipulation tableManipulation;
   private DataManipulationHelper dmHelper;

   @Override
   public void init(CacheLoaderConfiguration configuration, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      this.configuration = validateConfigurationClass(configuration, JdbcBinaryCacheStoreConfiguration.class);
      super.init(configuration, cache, m);
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      if (configuration.manageConnectionFactory()) {
         ConnectionFactory factory = ConnectionFactory.getConnectionFactory(configuration.connectionFactory().connectionFactoryClass());
         factory.start(configuration.connectionFactory(), factory.getClass().getClassLoader());
         doConnectionFactoryInitialization(factory);
      }
      dmHelper = new DataManipulationHelper(connectionFactory, tableManipulation, marshaller, timeService) {
         @Override
         protected String getLoadAllKeysSql() {
            return tableManipulation.getLoadAllKeysBinarySql();
         }

         @Override
         public void loadAllProcess(ResultSet rs, Set<InternalCacheEntry> result) throws SQLException, CacheLoaderException {
            Bucket bucket = unmarshallBucket(rs.getBinaryStream(1));
            long currentTimeMillis = timeService.wallClockTime();
            for (InternalCacheEntry ice: bucket.getStoredEntries()) {
               if (!ice.isExpired(currentTimeMillis)) {
                  result.add(ice);
               }
            }
         }

         @Override
         public void loadAllProcess(ResultSet rs, Set<InternalCacheEntry> result, int maxEntries) throws SQLException, CacheLoaderException {
            Bucket bucket = unmarshallBucket(rs.getBinaryStream(1));
            long currentTimeMillis = timeService.wallClockTime();
            for (InternalCacheEntry ice: bucket.getStoredEntries()) {
               if (!ice.isExpired(currentTimeMillis))
                  result.add(ice);

               if (result.size() == maxEntries)
                  break;
            }
         }

         @Override
         public void loadAllKeysProcess(ResultSet rs, Set<Object> keys, Set<Object> keysToExclude) throws SQLException, CacheLoaderException {
            Bucket bucket = unmarshallBucket(rs.getBinaryStream(1));
            long currentTimeMillis = timeService.wallClockTime();
            for (InternalCacheEntry ice: bucket.getStoredEntries()) {
               if (!ice.isExpired(currentTimeMillis) && includeKey(ice.getKey(), keysToExclude)) {
                  keys.add(ice.getKey());
               }
            }
         }

         @Override
         public void toStreamProcess(ResultSet rs, InputStream is, ObjectOutput objectOutput) throws CacheLoaderException, SQLException, IOException {
            Bucket bucket = unmarshallBucket(is);
            String bucketName = rs.getString(2);
            marshaller.objectToObjectStream(bucketName, objectOutput);
            marshaller.objectToObjectStream(bucket.getEntries(), objectOutput);
         }

         @Override
         public boolean fromStreamProcess(Object bucketName, PreparedStatement ps, ObjectInput objectInput)
               throws SQLException, CacheLoaderException, IOException, ClassNotFoundException, InterruptedException {
            if (bucketName instanceof String) {
               Map<Object, InternalCacheEntry> entries = (Map<Object, InternalCacheEntry>)
                     marshaller.objectFromObjectStream(objectInput);
               Bucket bucket = new Bucket(timeService, keyEquivalence, entries);
               ByteBuffer buffer = JdbcUtil.marshall(getMarshaller(), bucket.getEntries());
               ps.setBinaryStream(1, buffer.getStream(), buffer.getLength());
               ps.setLong(2, bucket.timestampOfFirstEntryToExpire());
               ps.setString(3, (String) bucketName);
               return true;
            } else {
               return false;
            }
         }
      };
   }

   @Override
   public void stop() throws CacheLoaderException {
      super.stop();

      Throwable cause = null;
      try {
         tableManipulation.stop();
      } catch (Throwable t) {
         cause = t;
         log.debug("Exception while stopping", t);
      }

      try {
         if (configuration.connectionFactory() instanceof ManagedConnectionFactory) {
            log.tracef("Stopping mananged connection factory: %s", connectionFactory);
            connectionFactory.stop();
         }
      } catch (Throwable t) {
         cause = t;
         log.debug("Exception while stopping", t);
      }
      if (cause != null) {
         throw new CacheLoaderException("Exceptions occurred while stopping store", cause);
      }
   }

   @Override
   protected void insertBucket(Bucket bucket) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManipulation.getInsertRowSql();
         ByteBuffer byteBuffer = JdbcUtil.marshall(getMarshaller(), bucket.getEntries());
         if (log.isTraceEnabled()) {
             log.tracef("Running insertBucket. Sql: '%s', on bucket: %s stored value size is %d bytes", sql, bucket, byteBuffer.getLength());
         }
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ps.setBinaryStream(1, byteBuffer.getStream(), byteBuffer.getLength());
         ps.setLong(2, bucket.timestampOfFirstEntryToExpire());
         ps.setString(3, bucket.getBucketIdAsString());
         int insertedRows = ps.executeUpdate();
         if (insertedRows != 1) {
            throw new CacheLoaderException("Unexpected insert result: '" + insertedRows + "'. Expected values is 1");
         }
      } catch (SQLException ex) {
         log.sqlFailureInsertingBucket(bucket, ex);
         throw new CacheLoaderException(String.format(
               "Sql failure while inserting bucket: %s", bucket), ex);
      } catch (InterruptedException ie) {
         if (log.isTraceEnabled()) {
            log.trace("Interrupted while marshalling to insert a bucket");
         }
         Thread.currentThread().interrupt();
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   @Override
   protected void updateBucket(Bucket bucket) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManipulation.getUpdateRowSql();
         if (log.isTraceEnabled()) {
            log.tracef("Running updateBucket. Sql: '%s', on bucket: %s", sql, bucket);
         }
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ByteBuffer buffer = JdbcUtil.marshall(getMarshaller(), bucket.getEntries());
         ps.setBinaryStream(1, buffer.getStream(), buffer.getLength());
         ps.setLong(2, bucket.timestampOfFirstEntryToExpire());
         ps.setString(3, bucket.getBucketIdAsString());
         int updatedRows = ps.executeUpdate();
         if (updatedRows != 1) {
            throw new CacheLoaderException("Unexpected  update result: '" + updatedRows + "'. Expected values is 1");
         }
      } catch (SQLException e) {
         log.sqlFailureUpdatingBucket(bucket, e);
         throw new CacheLoaderException(String.format(
               "Sql failure while updating bucket: %s", bucket), e);
      } catch (InterruptedException ie) {
         if (log.isTraceEnabled()) {
            log.trace("Interrupted while marshalling to update a bucket");
         }
         Thread.currentThread().interrupt();
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   @Override
   protected Bucket loadBucket(Integer keyHashCode) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         String sql = tableManipulation.getSelectRowSql();
         if (log.isTraceEnabled()) {
            log.tracef("Running loadBucket. Sql: '%s', on key: %s", sql, keyHashCode);
         }
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ps.setInt(1, keyHashCode);
         rs = ps.executeQuery();
         if (!rs.next()) {
            return null;
         }
         String bucketName = rs.getString(1);
         Bucket bucket = unmarshallBucket(rs.getBinaryStream(2));
         bucket.setBucketId(bucketName);
         return bucket;
      } catch (SQLException e) {
         log.sqlFailureLoadingKey(String.valueOf(keyHashCode), e);
         throw new CacheLoaderException(String.format(
               "Sql failure while loading key: %s", keyHashCode), e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   @Override
   public Set<InternalCacheEntry> loadAllLockSafe() throws CacheLoaderException {
      return dmHelper.loadAllSupport(false);
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      return dmHelper.loadAllKeysSupport(keysToExclude);
   }

   @Override
   protected Set<InternalCacheEntry> loadLockSafe(int maxEntries) throws CacheLoaderException {
      return dmHelper.loadSome(maxEntries);
   }

   @Override
   protected void loopOverBuckets(BucketHandler handler) throws CacheLoaderException {
      // this is a no-op.
      throw new UnsupportedOperationException("Should never be called.");
   }

   @Override
   protected void fromStreamLockSafe(ObjectInput objectInput) throws CacheLoaderException {
      dmHelper.fromStreamSupport(objectInput);
   }

   @Override
   protected void toStreamLockSafe(ObjectOutput objectOutput) throws CacheLoaderException {
      dmHelper.toStreamSupport(objectOutput, BINARY_STREAM_DELIMITER, false);
   }

   @Override
   protected void clearLockSafe() throws CacheLoaderException {
      dmHelper.clear();
   }

   @Override
   public void purgeInternal() throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      Set<Bucket> expiredBuckets = new HashSet<Bucket>();
      final int batchSize = 100;
      try {
         try {
            String sql = tableManipulation.getSelectExpiredRowsSql();
            conn = connectionFactory.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setLong(1, timeService.wallClockTime());
            rs = ps.executeQuery();
            while (rs.next()) {
               Integer key = rs.getInt(2);
               if (immediateLockForWriting(key)) {
                  if (log.isTraceEnabled()) {
                     log.tracef("Adding bucket keyed %s for purging.", key);
                  }
                  Bucket bucket;
                  try {
                     bucket = unmarshallBucket(rs.getBinaryStream(1));
                  } catch (Exception ex) {
                     // If something goes wrong during unmarshalling, unlock the
                     // key before rethrowing
                     unlock(key);
                     throw ex;
                  }
                  bucket.setBucketId(key);
                  expiredBuckets.add(bucket);
               } else {
                  if (log.isTraceEnabled()) {
                     log.tracef("Could not acquire write lock for %s, this won't be purged even though it has expired elements", key);
                  }
               }
            }
         } catch (Exception ex) {
            // if something happens make sure buckets locks are being release
            releaseLocks(expiredBuckets);
            log.failedClearingJdbcCacheStore(ex);
            throw new CacheLoaderException("Failed clearing JdbcBinaryCacheStore", ex);
         } finally {
            JdbcUtil.safeClose(ps);
            JdbcUtil.safeClose(rs);
         }

         if (log.isTraceEnabled()) {
            log.tracef("Found following buckets: %s which are about to be expired", expiredBuckets);
         }

         if (expiredBuckets.isEmpty()) {
            return;
         }

         Set<Bucket> emptyBuckets = new HashSet<Bucket>();
         // now update all the buckets in batch
         try {
            String sql = tableManipulation.getUpdateRowSql();
            ps = conn.prepareStatement(sql);
            int updateCount = 0;
            Iterator<Bucket> it = expiredBuckets.iterator();
            while (it.hasNext()) {
               Bucket bucket = it.next();
               bucket.removeExpiredEntries();
               if (!bucket.isEmpty()) {
                  ByteBuffer byteBuffer = JdbcUtil.marshall(getMarshaller(), bucket.getEntries());
                  ps.setBinaryStream(1, byteBuffer.getStream(), byteBuffer.getLength());
                  ps.setLong(2, bucket.timestampOfFirstEntryToExpire());
                  ps.setString(3, bucket.getBucketIdAsString());
                  ps.addBatch();
                  updateCount++;
                  if (updateCount % batchSize == 0) {
                     ps.executeBatch();
                     if (log.isTraceEnabled()) {
                        log.tracef("Flushing batch, update count is: %d", updateCount);
                     }
                  }
               } else {
                  it.remove();
                  emptyBuckets.add(bucket);
               }
            }
            // flush the batch
            if (updateCount % batchSize != 0) {
               if (log.isTraceEnabled()) {
                  log.tracef("Flushing batch, update count is: %d", updateCount);
               }
               ps.executeBatch();
            }
            if (log.isTraceEnabled()) {
               log.tracef("Updated %d buckets.", updateCount);
            }
         } catch (InterruptedException ie) {
            if (log.isTraceEnabled()) {
               log.trace("Interrupted while marshalling to purge expired entries");
            }
            Thread.currentThread().interrupt();
         } catch (Exception ex) {
            // if something happens make sure buckets locks are being release
            releaseLocks(emptyBuckets);
            log.failedClearingJdbcCacheStore(ex);
            throw new CacheLoaderException("Failed clearing JdbcBinaryCacheStore", ex);
         } finally {
            // release locks for the updated buckets.This won't include empty
            // buckets, as these were migrated to emptyBuckets
            releaseLocks(expiredBuckets);
            JdbcUtil.safeClose(ps);
         }

         if (log.isTraceEnabled()) {
            log.tracef("About to remove empty buckets %s", emptyBuckets);
         }

         if (emptyBuckets.isEmpty()) {
            return;
         }
         // then remove the empty buckets
         try {
            String sql = tableManipulation.getDeleteRowSql();
            ps = conn.prepareStatement(sql);
            int deletionCount = 0;
            for (Bucket bucket : emptyBuckets) {
               ps.setString(1, bucket.getBucketIdAsString());
               ps.addBatch();
               deletionCount++;
               if (deletionCount % batchSize == 0) {
                  if (log.isTraceEnabled()) {
                     log.tracef("Flushing deletion batch, total deletion count so far is %d", deletionCount);
                  }
                  ps.executeBatch();
               }
            }
            if (deletionCount % batchSize != 0) {
               int[] batchResult = ps.executeBatch();
               if (log.isTraceEnabled()) {
                  log.tracef("Flushed the batch and received following results: %s", Arrays.toString(batchResult));
               }
            }
         } catch (Exception ex) {
            // if something happens make sure buckets locks are being release
            log.failedClearingJdbcCacheStore(ex);
            throw new CacheLoaderException("Failed clearing JdbcBinaryCacheStore", ex);
         } finally {
            releaseLocks(emptyBuckets);
            JdbcUtil.safeClose(ps);
         }
      } finally {
         connectionFactory.releaseConnection(conn);
      }
   }

   private void releaseLocks(Set<Bucket> expiredBucketKeys) {
      for (Bucket bucket : expiredBucketKeys) {
         unlock(bucket.getBucketId());
      }
   }

   public ConnectionFactory getConnectionFactory() {
      return connectionFactory;
   }

   /**
    * Keeps a reference to the connection factory for further use. Also initializes the {@link
    * org.infinispan.loaders.jdbc.TableManipulation} that needs connections. This method should be called when you don't
    * want the store to manage the connection factory, perhaps because it is using an shared connection factory: see
    * {@link org.infinispan.loaders.jdbc.mixed.JdbcMixedCacheStore} for such an example of this.
    */
   public void doConnectionFactoryInitialization(ConnectionFactory connectionFactory) throws CacheLoaderException {
      this.connectionFactory = connectionFactory;
      this.tableManipulation = new TableManipulation(configuration.table());
      tableManipulation.setCacheName(cache.getName());
      tableManipulation.start(connectionFactory);
   }

   public TableManipulation getTableManipulation() {
      return tableManipulation;
   }

   @Override
   protected StreamingMarshaller getMarshaller() {
      return super.getMarshaller();
   }

   private Bucket unmarshallBucket(InputStream stream) throws CacheLoaderException {
      Map<Object, InternalCacheEntry> entries = JdbcUtil.unmarshall(getMarshaller(), stream);
      return new Bucket(timeService, keyEquivalence, entries);
   }

}
