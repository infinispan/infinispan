package org.infinispan.persistence.jdbc.binary;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.executors.ExecutorAllCompletionService;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.TableManipulation;
import org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.support.Bucket;
import org.infinispan.util.concurrent.locks.StripedLock;
import org.infinispan.util.logging.LogFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

/**
 * {@link org.infinispan.persistence.spi.AdvancedLoadWriteStore} implementation that will store all the buckets as rows
 * in database, each row corresponding to a bucket. This is in contrast to {@link org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore}
 * which stores each StoredEntry as a row in the database. </p> It is generally recommended to use {@link
 * org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore} whenever possible as it performs better. Please
 * read {@link org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore}'s javadoc for more details on this.
 * <p/>
 * This class has the benefit of being able to store StoredEntries that do not have String keys, at the cost of coarser
 * grained access granularity, and inherently performance.
 * <p/>
 * All the DB related configurations are described in {@link org.infinispan.persistence.jdbc.binary
 * .JdbcBinaryStoreConfiguration}.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfiguration
 * @see org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore
 */
public class JdbcBinaryStore implements AdvancedLoadWriteStore {

   private static final Log log = LogFactory.getLog(JdbcBinaryStore.class, Log.class);

   private StripedLock locks;

   private JdbcBinaryStoreConfiguration configuration;

   private ConnectionFactory connectionFactory;
   private TableManipulation tableManipulation;
   private InitializationContext ctx;
   private Equivalence<Object> keyEquivalence;

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
   }

   @Override
   public void start() {
      locks = new StripedLock(configuration.lockConcurrencyLevel());
      if (configuration.manageConnectionFactory()) {
         ConnectionFactory factory = ConnectionFactory.getConnectionFactory(configuration.connectionFactory().connectionFactoryClass());
         factory.start(configuration.connectionFactory(), factory.getClass().getClassLoader());
         doConnectionFactoryInitialization(factory);
      }
      keyEquivalence = ctx.getCache().getCacheConfiguration().dataContainer().keyEquivalence();
   }

   @Override
   public void stop() {
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
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }
      if (cause != null) {
         throw new CacheLoaderException("Exceptions occurred while stopping store", cause);
      }
   }

   @Override
   public final void write(MarshalledEntry entry) {
      log.tracef("store(%s)", entry);
      InternalMetadata m = entry.getMetadata();

      if (m != null && m.isExpired(ctx.getTimeService().wallClockTime())) {
         delete(entry.getKey());
         return;
      }

      Integer bucketId = getBuckedId(entry.getKey());
      lockBucketForWriting(bucketId);
      try {
         storeInBucket(entry, bucketId);
      } finally {
         unlock(bucketId);
      }
   }

   @Override
   public final MarshalledEntry load(Object key) {
      Integer bucketId = getBuckedId(key);
      lockBucketForReading(bucketId);
      try {
         Bucket bucket = loadBucket(bucketId);
         if (bucket == null) {
            return null;
         }
         return bucket.getEntry(key, ctx.getTimeService());
      } finally {
         unlock(bucketId);
      }
   }

   @Override
   public boolean contains(Object key) {
      Integer bucketId = getBuckedId(key);
      lockBucketForReading(bucketId);
      try {
         Bucket bucket = loadBucket(bucketId);
         return bucket != null && bucket.contains(key, ctx.getTimeService());
      } finally {
         unlock(bucketId);
      }
   }

   @Override
   public final boolean delete(Object key) {
      log.tracef("delete(%s)", key);
      Integer bucketId = getBuckedId(key);
      try {
         lockBucketForWriting(bucketId);
         return removeKeyFromBucket(key, bucketId);
      } finally {
         unlock(bucketId);
         log.tracef("Exit delete(%s)", key);
      }
   }

   @Override
   public void process(final KeyFilter filter, final CacheLoaderTask task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
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
         ExecutorAllCompletionService ecs = new ExecutorAllCompletionService(executor);
         final TaskContextImpl taskContext = new TaskContextImpl();
         //we can do better here: ATM we load the entries in the caller's thread and process them in parallel
         // we can do the loading (expensive operation) in parallel as well.
         while (rs.next()) {
            InputStream binaryStream = rs.getBinaryStream(1);
            final Bucket bucket = unmarshallBucket(binaryStream);
            ecs.submit(new Callable<Void>() {
               @Override
               public Void call() throws Exception {
                  try {
                     for (MarshalledEntry me : bucket.getStoredEntries(filter, ctx.getTimeService()).values()) {
                        if (!taskContext.isStopped()) {
                           task.processEntry(me, taskContext);
                        }
                     }
                     return null;
                  } catch (Exception e) {
                     log.errorExecutingParallelStoreTask(e);
                     throw e;
                  }
               }
            });
         }
         ecs.waitUntilAllCompleted();
         if (ecs.isExceptionThrown()) {
            throw new CacheLoaderException("Execution exception!", ecs.getFirstException());
         }
      } catch (SQLException e) {
         log.sqlFailureFetchingAllStoredEntries(e);
         throw new CacheLoaderException("SQL error while fetching all StoredEntries", e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   @Override
   public void clear() {
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
   public int size() {
      return PersistenceUtil.count(this, null);
   }

   @Override
   public void purge(Executor threadPool, PurgeListener task) {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      Set<Bucket> expiredBuckets = new HashSet<Bucket>();
      final int batchSize = 100;
      ExecutorAllCompletionService eacs = new ExecutorAllCompletionService(threadPool);
      Set<Bucket> emptyBuckets = new HashSet<Bucket>(batchSize);
      int taskCount = 0;
      try {
         try {
            String sql = tableManipulation.getSelectExpiredRowsSql();
            conn = connectionFactory.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setLong(1, ctx.getTimeService().wallClockTime());
            rs = ps.executeQuery();
            while (rs.next()) {
               Integer bucketId = rs.getInt(2);
               if (immediateLockForWriting(bucketId)) {
                  if (log.isTraceEnabled()) {
                     log.tracef("Adding bucket keyed %s for purging.", bucketId);
                  }
                  Bucket bucket;
                  InputStream binaryStream = rs.getBinaryStream(1);
                  bucket = unmarshallBucket(binaryStream);
                  bucket.setBucketId(bucketId);
                  expiredBuckets.add(bucket);
                  if (expiredBuckets.size() == batchSize) {
                     eacs.submit(new BucketPurger(expiredBuckets, task, ctx.getMarshaller(), conn, emptyBuckets));
                     taskCount++;
                     expiredBuckets = new HashSet<Bucket>(batchSize);
                  }
               } else {
                  if (log.isTraceEnabled()) {
                     log.tracef("Could not acquire write lock for %s, this won't be purged even though it has expired elements", bucketId);
                  }
               }
            }

            if (!expiredBuckets.isEmpty())
               eacs.submit(new BucketPurger(expiredBuckets, task, ctx.getMarshaller(), conn, emptyBuckets));

         } catch (Exception ex) {
            // if something happens make sure buckets locks are being release
            releaseLocks(expiredBuckets);
            log.failedClearingJdbcCacheStore(ex);
            throw new CacheLoaderException("Failed clearing JdbcBinaryStore", ex);
         } finally {
            JdbcUtil.safeClose(ps);
            JdbcUtil.safeClose(rs);
         }


         eacs.waitUntilAllCompleted();
         if (eacs.isExceptionThrown()) {
            releaseLocks(emptyBuckets);
         }

         if (emptyBuckets.isEmpty())
            return;

         if (log.isTraceEnabled()) {
            log.tracef("About to remove empty buckets %s", emptyBuckets);
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
            throw new CacheLoaderException("Failed clearing JdbcBinaryStore", ex);
         } finally {
            releaseLocks(emptyBuckets);
            JdbcUtil.safeClose(ps);
         }
      } finally {
         connectionFactory.releaseConnection(conn);
      }
   }

   private class BucketPurger implements Callable<Void> {

      private final Collection<Bucket> buckets;
      private final PurgeListener purgeListener;
      private final StreamingMarshaller marshaller;
      private final Connection conn;
      private final Collection<Bucket> emptyBuckets;

      private BucketPurger(Collection<Bucket> buckets, PurgeListener purgeListener, StreamingMarshaller marshaller,
                           Connection conn, Collection<Bucket> emptyBuckets) {
         this.buckets = buckets;
         this.purgeListener = purgeListener;
         this.marshaller = marshaller;
         this.conn = conn;
         this.emptyBuckets = emptyBuckets;
      }

      @Override
      public Void call() throws Exception {
         PreparedStatement ps = null;
         try {
            String sql = tableManipulation.getUpdateRowSql();
            ps = conn.prepareStatement(sql);
            for (Iterator<Bucket> it = buckets.iterator(); it.hasNext();) {
               Bucket bucket = it.next();
               for (Object key : bucket.removeExpiredEntries(ctx.getTimeService())) {
                  if (purgeListener != null) purgeListener.entryPurged(key);
               }
               if (!bucket.isEmpty()) {
                  ByteBuffer byteBuffer = JdbcUtil.marshall(marshaller, bucket);
                  ps.setBinaryStream(1, byteBuffer.getStream(), byteBuffer.getLength());
                  ps.setLong(2, bucket.timestampOfFirstEntryToExpire());
                  ps.setString(3, bucket.getBucketIdAsString());
                  ps.addBatch();
               } else {
                  it.remove();
                  emptyBuckets.add(bucket);
               }
            }
            ps.executeBatch();
         } finally {
            JdbcUtil.safeClose(ps);
            releaseLocks(buckets);
         }
         return null;
      }
   }

   protected void insertBucket(Bucket bucket) {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManipulation.getInsertRowSql();
         ByteBuffer byteBuffer = JdbcUtil.marshall(ctx.getMarshaller(), bucket.getStoredEntries());
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

   protected void updateBucket(Bucket bucket) {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManipulation.getUpdateRowSql();
         if (log.isTraceEnabled()) {
            log.tracef("Running updateBucket. Sql: '%s', on bucket: %s", sql, bucket);
         }
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ByteBuffer buffer = JdbcUtil.marshall(ctx.getMarshaller(), bucket.getStoredEntries());
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

   protected Bucket loadBucket(Integer bucketId) {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         String sql = tableManipulation.getSelectRowSql();
         if (log.isTraceEnabled()) {
            log.tracef("Running loadBucket. Sql: '%s', on key: %s", sql, bucketId);
         }
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ps.setInt(1, bucketId);
         rs = ps.executeQuery();
         if (!rs.next()) {
            return null;
         }
         String bucketName = rs.getString(1);
         InputStream inputStream = rs.getBinaryStream(2);
         Bucket bucket = unmarshallBucket(inputStream);
         bucket.setBucketId(bucketName);//bucket name is volatile, so not persisted.
         return bucket;
      } catch (SQLException e) {
         log.sqlFailureLoadingKey(String.valueOf(bucketId), e);
         throw new CacheLoaderException(String.format(
               "Sql failure while loading key: %s", bucketId), e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   private void releaseLocks(Collection<Bucket> expiredBucketKeys) {
      for (Bucket bucket : expiredBucketKeys) {
         unlock(bucket.getBucketId());
      }
   }

   public ConnectionFactory getConnectionFactory() {
      return connectionFactory;
   }

   public void doConnectionFactoryInitialization(ConnectionFactory connectionFactory) {
      this.connectionFactory = connectionFactory;
      this.tableManipulation = new TableManipulation(configuration.table());
      tableManipulation.setCacheName(ctx.getCache().getName());
      tableManipulation.start(connectionFactory);
   }

   public TableManipulation getTableManipulation() {
      return tableManipulation;
   }

   protected void storeInBucket(MarshalledEntry me, Integer bucketId) {
      Bucket bucket = loadBucket(bucketId);
      if (bucket != null) {
         bucket.addEntry(me.getKey(), me);
         updateBucket(bucket);
      } else {
         bucket = new Bucket(keyEquivalence);
         bucket.setBucketId(bucketId);
         bucket.addEntry(me.getKey(), me);
         insertBucket(bucket);
      }
   }

   protected boolean removeKeyFromBucket(Object key, Integer bucketId) {
      Bucket bucket = loadBucket(bucketId);
      if (bucket == null) {
         return false;
      } else {
         boolean success = bucket.removeEntry(key);
         if (success) {
            updateBucket(bucket);
         }
         return success;
      }
   }

   public Integer getBuckedId(Object key) {
      return keyEquivalence.hashCode(key) & 0xfffffc00; // To reduce the number of buckets/locks that may be created.
   }


   /**
    * Release the locks (either read or write).
    */
   protected final void unlock(Integer key) {
      locks.releaseLock(key);
   }

   /**
    * Acquires write lock on the given key.
    */
   protected final void lockBucketForWriting(Integer key) {
      locks.acquireLock(key, true);
   }

   /**
    * Acquires read lock on the given key.
    */
   protected final void lockBucketForReading(Integer bucket) {
      locks.acquireLock(bucket, false);
   }

   protected final boolean immediateLockForWriting(Integer key) {
      return locks.acquireLock(key, true, 0);
   }

   public JdbcBinaryStoreConfiguration getConfiguration() {
      return configuration;
   }

   private Bucket unmarshallBucket(InputStream stream) throws CacheLoaderException {
      Map<Object, MarshalledEntry> entries = JdbcUtil.unmarshall(ctx.getMarshaller(), stream);
      return new Bucket(entries, keyEquivalence);
   }

}
