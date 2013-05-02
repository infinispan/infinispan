/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.jdbc.binary;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.io.ByteBuffer;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.loaders.bucket.BucketBasedCacheStore;
import org.infinispan.loaders.jdbc.DataManipulationHelper;
import org.infinispan.loaders.jdbc.JdbcUtil;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.logging.Log;
import org.infinispan.marshall.StreamingMarshaller;
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
 * All the DB related configurations are described in {@link org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStoreConfig}.
 *
 * @author Mircea.Markus@jboss.com
 * @see JdbcBinaryCacheStoreConfig
 * @see org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore
 */
@CacheLoaderMetadata(configurationClass = JdbcBinaryCacheStoreConfig.class)
public class JdbcBinaryCacheStore extends BucketBasedCacheStore {

   private static final Log log = LogFactory.getLog(JdbcBinaryCacheStore.class, Log.class);

   private final static byte BINARY_STREAM_DELIMITER = 100;

   private JdbcBinaryCacheStoreConfig config;
   private ConnectionFactory connectionFactory;
   TableManipulation tableManipulation;
   private DataManipulationHelper dmHelper;
   private String cacheName;

   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      if (log.isTraceEnabled()) {
         log.tracef("Initializing JdbcBinaryCacheStore %s", config);
      }
      super.init(config, cache, m);
      this.config = (JdbcBinaryCacheStoreConfig) config;
      cacheName = cache.getName();
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      String connectionFactoryClass = config.getConnectionFactoryConfig().getConnectionFactoryClass();
      if (config.isManageConnectionFactory()) {
         ConnectionFactory factory = ConnectionFactory.getConnectionFactory(connectionFactoryClass, config.getClassLoader());
         factory.start(config.getConnectionFactoryConfig(), config.getClassLoader());
         doConnectionFactoryInitialization(factory);
      }
      dmHelper = new DataManipulationHelper(connectionFactory, tableManipulation, marshaller, timeService) {
         @Override
         protected String getLoadAllKeysSql() {
            return tableManipulation.getLoadAllKeysBinarySql();
         }

         @Override
         public void loadAllProcess(ResultSet rs, Set<InternalCacheEntry> result) throws SQLException, CacheLoaderException {
            InputStream binaryStream = rs.getBinaryStream(1);
            Bucket bucket = (Bucket) JdbcUtil.unmarshall(getMarshaller(), binaryStream);
            long currentTimeMillis = timeService.wallClockTime();
            for (InternalCacheEntry ice: bucket.getStoredEntries()) {
               if (!ice.isExpired(currentTimeMillis)) {
                  result.add(ice);
               }
            }
         }

         @Override
         public void loadAllProcess(ResultSet rs, Set<InternalCacheEntry> result, int maxEntries) throws SQLException, CacheLoaderException {
            InputStream binaryStream = rs.getBinaryStream(1);
            Bucket bucket = (Bucket) JdbcUtil.unmarshall(getMarshaller(), binaryStream);
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
            InputStream binaryStream = rs.getBinaryStream(1);
            Bucket bucket = (Bucket) JdbcUtil.unmarshall(getMarshaller(), binaryStream);
            long currentTimeMillis = timeService.wallClockTime();
            for (InternalCacheEntry ice: bucket.getStoredEntries()) {
               if (!ice.isExpired(currentTimeMillis) && includeKey(ice.getKey(), keysToExclude)) {
                  keys.add(ice.getKey());
               }
            }
         }

         @Override
         public void toStreamProcess(ResultSet rs, InputStream is, ObjectOutput objectOutput) throws CacheLoaderException, SQLException, IOException {
            Bucket bucket = (Bucket) JdbcUtil.unmarshall(getMarshaller(), is);
            String bucketName = rs.getString(2);
            marshaller.objectToObjectStream(bucketName, objectOutput);
            marshaller.objectToObjectStream(bucket, objectOutput);
         }

         @Override
         public boolean fromStreamProcess(Object bucketName, PreparedStatement ps, ObjectInput objectInput)
               throws SQLException, CacheLoaderException, IOException, ClassNotFoundException, InterruptedException {
            if (bucketName instanceof String) {
               Bucket bucket = (Bucket) marshaller.objectFromObjectStream(objectInput);
               ByteBuffer buffer = JdbcUtil.marshall(getMarshaller(), bucket);
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
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }

      try {
         if (config.isManageConnectionFactory()) {
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
   protected void insertBucket(Bucket bucket) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManipulation.getInsertRowSql();
         ByteBuffer byteBuffer = JdbcUtil.marshall(getMarshaller(), bucket);
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
         ByteBuffer buffer = JdbcUtil.marshall(getMarshaller(), bucket);
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
         InputStream inputStream = rs.getBinaryStream(2);
         Bucket bucket = (Bucket) JdbcUtil.unmarshall(getMarshaller(), inputStream);
         bucket.setBucketId(bucketName);//bucket name is volatile, so not persisted.
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
                  Bucket bucket = null;
                  try {
                     InputStream binaryStream = rs.getBinaryStream(1);
                     bucket = (Bucket) JdbcUtil.unmarshall(getMarshaller(), binaryStream);
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
                  ByteBuffer byteBuffer = JdbcUtil.marshall(getMarshaller(), bucket);
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

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return JdbcBinaryCacheStoreConfig.class;
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
      tableManipulation = config.getTableManipulation();
      tableManipulation.setCacheName(cacheName);
      tableManipulation.start(connectionFactory);
   }

   public TableManipulation getTableManipulation() {
      return tableManipulation;
   }

   @Override
   protected StreamingMarshaller getMarshaller() {
      return super.getMarshaller();
   }
}
