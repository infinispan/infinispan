package org.horizon.loader.jdbc.binary;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.horizon.Cache;
import org.horizon.io.ByteBuffer;
import org.horizon.loader.CacheLoaderConfig;
import org.horizon.loader.CacheLoaderException;
import org.horizon.loader.StoredEntry;
import org.horizon.loader.bucket.Bucket;
import org.horizon.loader.bucket.BucketBasedCacheStore;
import org.horizon.loader.jdbc.JdbcUtil;
import org.horizon.loader.jdbc.TableManipulation;
import org.horizon.loader.jdbc.connectionfactory.ConnectionFactory;
import org.horizon.marshall.Marshaller;

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
 * coresponding to a bucket. This is in contrast to {@link org.horizon.loader.jdbc.stringbased.JdbcStringBasedCacheStore}
 * which stores each StoredEntry as an row in the database.
 * <p/>
 * This class has the benefit of being able to store StoredEntries that do not have String keys, at the cost of coarser
 * grained access granularity, and inherently performance.
 * <p/>
 * All the DB releated configurations are described in {@link org.horizon.loader.jdbc.binary.JdbcBinaryCacheStoreConfig}.
 *
 * @author Mircea.Markus@jboss.com
 * @see JdbcBinaryCacheStoreConfig
 * @see org.horizon.loader.jdbc.stringbased.JdbcStringBasedCacheStore
 */
public class JdbcBinaryCacheStore extends BucketBasedCacheStore {

   private static final Log log = LogFactory.getLog(JdbcBinaryCacheStore.class);
   private final static String BINARY_STREAM_DELIMITER = "__JdbcBinaryCacheStore_done__";

   private JdbcBinaryCacheStoreConfig config;
   private ConnectionFactory connectionFactory;
   private TableManipulation tableManipulation;

   public void init(CacheLoaderConfig config, Cache cache, Marshaller m) {
      if (log.isTraceEnabled())
         log.trace("Initializing JdbcBinaryCacheStore " + config);
      super.init(config, cache, m);
      this.config = (JdbcBinaryCacheStoreConfig) config;
   }

   public void start() throws CacheLoaderException {
      super.start();
      String connectionFactoryClass = config.getConnectionFactoryConfig().getConnectionFactoryClass();
      if (config.isManageConnectionFatory()) {
         ConnectionFactory factory = ConnectionFactory.getConnectionFactory(connectionFactoryClass);
         factory.start(config.getConnectionFactoryConfig());
         doConnectionFactoryInitialization(factory);
      }
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
         logAndThrow(ex, "sql failure while inserting bucket: " + bucket);
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   protected void saveBucket(Bucket bucket) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManipulation.getUpdateRowSql();
         if (log.isTraceEnabled()) {
            log.trace("Running saveBucket. Sql: '" + sql + "', on bucket: " + bucket);
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
         logAndThrow(e, "sql failure while updating bucket: " + bucket);
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

   public Set<StoredEntry> loadAllLockSafe() throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         String sql = tableManipulation.getLoadAllRowsSql();
         if (log.isTraceEnabled()) {
            log.trace("Running loadAll. Sql: '" + sql + "'");
         }
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         rs = ps.executeQuery();
         rs.setFetchSize(config.getFetchSize());
         Set<StoredEntry> result = new HashSet<StoredEntry>();
         while (rs.next()) {
            InputStream binaryStream = rs.getBinaryStream(1);
            Bucket bucket = (Bucket) JdbcUtil.unmarshall(getMarshaller(), binaryStream);
            result.addAll(bucket.getStoredEntries());
         }
         return result;
      } catch (SQLException e) {
         String message = "sql failure while loading key: ";
         log.error(message, e);
         throw new CacheLoaderException(message, e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   protected void fromStreamLockSafe(ObjectInput objectInput) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         conn = connectionFactory.getConnection();
         String sql = tableManipulation.getInsertRowSql();
         ps = conn.prepareStatement(sql);

         int readBuckets = 0;
         int batchSize = config.getBatchSize();
         String bucketName = (String) objectInput.readObject();
         while (!bucketName.equals(BINARY_STREAM_DELIMITER)) {
            Bucket bucket = (Bucket) objectInput.readObject();
            readBuckets++;
            ByteBuffer buffer = JdbcUtil.marshall(getMarshaller(), bucket);
            ps.setBinaryStream(1, buffer.getStream(), buffer.getLength());
            ps.setLong(2, bucket.timestampOfFirstEntryToExpire());
            ps.setString(3, bucketName);
            if (readBuckets % batchSize == 0) {
               ps.executeBatch();
               if (log.isTraceEnabled())
                  log.trace("Executing batch " + (readBuckets / batchSize) + ", batch size is " + batchSize);
            } else {
               ps.addBatch();
            }
            bucketName = (String) objectInput.readObject();
         }
         if (readBuckets % batchSize != 0)
            ps.executeBatch();//flush the batch
         if (log.isTraceEnabled())
            log.trace("Successfully inserted " + readBuckets + " buckets into the database, batch size is " + batchSize);
      } catch (IOException ex) {
         logAndThrow(ex, "I/O failure while integrating state into store");
      } catch (SQLException e) {
         logAndThrow(e, "SQL failure while integrating state into store");
      } catch (ClassNotFoundException e) {
         logAndThrow(e, "Unexpected failure while integrating state into store");
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   protected void toStreamLockSafe(ObjectOutput objectOutput) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         conn = connectionFactory.getConnection();
         String sql = tableManipulation.getLoadAllRowsSql();
         ps = conn.prepareStatement(sql);
         rs = ps.executeQuery();
         rs.setFetchSize(config.getFetchSize());
         while (rs.next()) {
            InputStream inputStream = rs.getBinaryStream(1);
            Bucket bucket = (Bucket) JdbcUtil.unmarshall(getMarshaller(), inputStream);
            String bucketName = rs.getString(2);
            objectOutput.writeObject(bucketName);
            objectOutput.writeObject(bucket);
         }
         objectOutput.writeObject(BINARY_STREAM_DELIMITER);
      } catch (SQLException ex) {
         logAndThrow(ex, "SQL failure while writing store's content to stream");
      }
      catch (IOException e) {
         logAndThrow(e, "IO failure while writing store's content to stream");
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   protected void clearLockSafe() throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManipulation.getDeleteAllRowsSql();
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         int result = ps.executeUpdate();
         if (log.isTraceEnabled())
            log.trace("Successfully removed " + result + " rows.");
      } catch (SQLException ex) {
         logAndThrow(ex, "Failed clearing JdbcBinaryCacheStore");
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

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
            if (immediateLockForWritting(key)) {
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
         logAndThrow(ex, "Failed clearing JdbcBinaryCacheStore");
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
         logAndThrow(ex, "Failed clearing JdbcBinaryCacheStore");
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
         logAndThrow(ex, "Failed clearing JdbcBinaryCacheStore");
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

   protected void logAndThrow(Exception e, String message) throws CacheLoaderException {
      log.error(message, e);
      throw new CacheLoaderException(message, e);
   }

   public ConnectionFactory getConnectionFactory() {
      return connectionFactory;
   }

   /**
    * Keeps a reference to the connection factory for further use. Also initializes the {@link
    * org.horizon.loader.jdbc.TableManipulation} that needs connections. This method should be called when you don't
    * want the store to manage the connection factory, perhaps because it is using an shared connection factory: see
    * {@link org.horizon.loader.jdbc.mixed.JdbcMixedCacheStore} for such an example of this.
    */
   public void doConnectionFactoryInitialization(ConnectionFactory connectionFactory) throws CacheLoaderException {
      this.connectionFactory = connectionFactory;
      tableManipulation = config.getTableManipulation();
      tableManipulation.start(connectionFactory);
   }
}
