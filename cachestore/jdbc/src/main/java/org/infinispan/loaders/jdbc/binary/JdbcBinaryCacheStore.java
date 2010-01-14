/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.loaders.bucket.BucketBasedCacheStore;
import org.infinispan.loaders.jdbc.DataManipulationHelper;
import org.infinispan.loaders.jdbc.JdbcUtil;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.logging.Log;
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
public class JdbcBinaryCacheStore extends BucketBasedCacheStore {

   private static final Log log = LogFactory.getLog(JdbcBinaryCacheStore.class);

   private final static byte BINARY_STREAM_DELIMITER = 100;

   private JdbcBinaryCacheStoreConfig config;
   private ConnectionFactory connectionFactory;
   private TableManipulation tableManipulation;
   private DataManipulationHelper dmHelper;
   private String cacheName;

   public void init(CacheLoaderConfig config, Cache cache, Marshaller m) throws CacheLoaderException {
      if (log.isTraceEnabled())
         log.trace("Initializing JdbcBinaryCacheStore " + config);
      super.init(config, cache, m);
      this.config = (JdbcBinaryCacheStoreConfig) config;
      this.cacheName = cache.getName();
   }

   public void start() throws CacheLoaderException {
      super.start();
      String connectionFactoryClass = config.getConnectionFactoryConfig().getConnectionFactoryClass();
      if (config.isManageConnectionFatory()) {
         ConnectionFactory factory = ConnectionFactory.getConnectionFactory(connectionFactoryClass);
         factory.start(config.getConnectionFactoryConfig());
         doConnectionFactoryInitialization(factory);
      }
      dmHelper = new DataManipulationHelper(connectionFactory, tableManipulation, marshaller) {
         @Override
         public void loadAllProcess(ResultSet rs, Set<InternalCacheEntry> result) throws SQLException, CacheLoaderException {
            InputStream binaryStream = rs.getBinaryStream(1);
            Bucket bucket = (Bucket) JdbcUtil.unmarshall(getMarshaller(), binaryStream);
            result.addAll(bucket.getStoredEntries());
         }

         @Override
         public void toStreamProcess(ResultSet rs, InputStream is, ObjectOutput objectOutput) throws CacheLoaderException, SQLException, IOException {
            Bucket bucket = (Bucket) JdbcUtil.unmarshall(getMarshaller(), is);
            String bucketName = rs.getString(2);
            marshaller.objectToObjectStream(bucketName, objectOutput);
            marshaller.objectToObjectStream(bucket, objectOutput);
         }

         public boolean fromStreamProcess(Object bucketName, PreparedStatement ps, ObjectInput objectInput) throws SQLException, CacheLoaderException, IOException, ClassNotFoundException {
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

   public void stop() throws CacheLoaderException {
      tableManipulation.stop();
      if (config.isManageConnectionFatory()) {
         connectionFactory.stop();
      }
   }

   protected void insertBucket(Bucket bucket) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManipulation.getInsertRowSql();
         if (log.isTraceEnabled()) {
            log.trace("Running insertBucket. Sql: '" + sql + "', on bucket: " + bucket);
         }
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ByteBuffer byteBuffer = JdbcUtil.marshall(getMarshaller(), bucket);
         ps.setBinaryStream(1, byteBuffer.getStream(), byteBuffer.getLength());
         ps.setLong(2, bucket.timestampOfFirstEntryToExpire());
         ps.setString(3, bucket.getBucketName());
         int insertedRows = ps.executeUpdate();
         if (insertedRows != 1) {
            throw new CacheLoaderException("Unexpected insert result: '" + insertedRows + "'. Expected values is 1");
         }
      } catch (SQLException ex) {
         DataManipulationHelper.logAndThrow(ex, "sql failure while inserting bucket: " + bucket);
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   protected void updateBucket(Bucket bucket) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManipulation.getUpdateRowSql();
         if (log.isTraceEnabled()) {
            log.trace("Running updateBucket. Sql: '" + sql + "', on bucket: " + bucket);
         }
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ByteBuffer buffer = JdbcUtil.marshall(getMarshaller(), bucket);
         ps.setBinaryStream(1, buffer.getStream(), buffer.getLength());
         ps.setLong(2, bucket.timestampOfFirstEntryToExpire());
         ps.setString(3, bucket.getBucketName());
         int updatedRows = ps.executeUpdate();
         if (updatedRows != 1) {
            throw new CacheLoaderException("Unexpected  update result: '" + updatedRows + "'. Expected values is 1");
         }
      } catch (SQLException e) {
         DataManipulationHelper.logAndThrow(e, "sql failure while updating bucket: " + bucket);
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   protected Bucket loadBucket(String keyHashCode) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         String sql = tableManipulation.getSelectRowSql();
         if (log.isTraceEnabled()) {
            log.trace("Running loadBucket. Sql: '" + sql + "', on key: " + keyHashCode);
         }
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ps.setString(1, keyHashCode);
         rs = ps.executeQuery();
         if (!rs.next()) return null;
         String bucketName = rs.getString(1);
         InputStream inputStream = rs.getBinaryStream(2);
         Bucket bucket = (Bucket) JdbcUtil.unmarshall(getMarshaller(), inputStream);
         bucket.setBucketName(bucketName);//bucket name is volatile, so not persisted.
         return bucket;
      } catch (SQLException e) {
         String message = "sql failure while loading key: " + keyHashCode;
         log.error(message, e);
         throw new CacheLoaderException(message, e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   public Set<InternalCacheEntry> loadAllLockSafe() throws CacheLoaderException {
      return dmHelper.loadAllSupport();
   }

   protected void fromStreamLockSafe(ObjectInput objectInput) throws CacheLoaderException {
      dmHelper.fromStreamSupport(objectInput);
   }

   protected void toStreamLockSafe(ObjectOutput objectOutput) throws CacheLoaderException {
      dmHelper.toStreamSupport(objectOutput, BINARY_STREAM_DELIMITER);
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
         String sql = tableManipulation.getSelectExpiredRowsSql();
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ps.setLong(1, System.currentTimeMillis());
         rs = ps.executeQuery();
         while (rs.next()) {
            String key = rs.getString(2);
            if (immediateLockForWriting(key)) {
               if (log.isTraceEnabled()) log.trace("Adding bucket keyed " + key + " for purging.");
               InputStream binaryStream = rs.getBinaryStream(1);
               Bucket bucket = (Bucket) JdbcUtil.unmarshall(getMarshaller(), binaryStream);
               bucket.setBucketName(key);
               expiredBuckets.add(bucket);
            } else {
               if (log.isTraceEnabled())
                  log.trace("Could not acquire write lock for " + key + ", this won't be purged even though it has expired elements");
            }
         }
      } catch (SQLException ex) {
         //if something happens make sure buckets locks are being release
         releaseLocks(expiredBuckets);
         connectionFactory.releaseConnection(conn);
         DataManipulationHelper.logAndThrow(ex, "Failed clearing JdbcBinaryCacheStore");
      } finally {
         JdbcUtil.safeClose(ps);
         JdbcUtil.safeClose(rs);
      }

      if (log.isTraceEnabled())
         log.trace("Found following buckets: " + expiredBuckets + " which are about to be expired");

      if (expiredBuckets.isEmpty()) return;
      Set<Bucket> emptyBuckets = new HashSet<Bucket>();
      //now update all the buckets in batch
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
               ps.addBatch();
               updateCount++;
               if (updateCount % batchSize == 0) {
                  ps.executeBatch();
                  if (log.isTraceEnabled()) log.trace("Flushing batch, update count is: " + updateCount);
               }
            } else {
               it.remove();
               emptyBuckets.add(bucket);
            }
         }
         //flush the batch
         if (updateCount % batchSize != 0) {
            ps.executeBatch();
         }
         if (log.isTraceEnabled()) log.trace("Updated " + updateCount + " buckets.");
      } catch (SQLException ex) {
         //if something happens make sure buckets locks are being release
         releaseLocks(emptyBuckets);
         connectionFactory.releaseConnection(conn);
         DataManipulationHelper.logAndThrow(ex, "Failed clearing JdbcBinaryCacheStore");
      } finally {
         //release locks for the updated buckets.This won't include empty buckets, as these were migrated to emptyBuckets
         releaseLocks(expiredBuckets);
         JdbcUtil.safeClose(ps);
      }


      if (log.isTraceEnabled()) log.trace("About to remove empty buckets " + emptyBuckets);

      if (emptyBuckets.isEmpty()) return;
      //then remove the empty buckets
      try {
         String sql = tableManipulation.getDeleteRowSql();
         ps = conn.prepareStatement(sql);
         int deletionCount = 0;
         for (Bucket bucket : emptyBuckets) {
            ps.setString(1, bucket.getBucketName());
            ps.addBatch();
            deletionCount++;
            if (deletionCount % batchSize == 0) {
               if (log.isTraceEnabled())
                  log.trace("Flushing deletion batch, total deletion count so far is " + deletionCount);
               ps.executeBatch();
            }
         }
         if (deletionCount % batchSize != 0) {
            int[] batchResult = ps.executeBatch();
            if (log.isTraceEnabled())
               log.trace("Flushed the batch and received following results: " + Arrays.toString(batchResult));
         }
      } catch (SQLException ex) {
         //if something happens make sure buckets locks are being release
         DataManipulationHelper.logAndThrow(ex, "Failed clearing JdbcBinaryCacheStore");
      } finally {
         releaseLocks(emptyBuckets);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   private void releaseLocks(Set<Bucket> expiredBucketKeys) throws CacheLoaderException {
      for (Bucket bucket : expiredBucketKeys) {
         this.unlock(bucket.getBucketName());
      }
   }

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
}
