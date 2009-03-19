package org.horizon.loader.jdbc.stringbased;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.horizon.Cache;
import org.horizon.io.ByteBuffer;
import org.horizon.loader.CacheLoaderConfig;
import org.horizon.loader.CacheLoaderException;
import org.horizon.loader.LockSupportCacheStore;
import org.horizon.loader.StoredEntry;
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
import java.util.HashSet;
import java.util.Set;

/**
 * {@link org.horizon.loader.CacheStore} implementation that stores the entries in a database. In contrast to the {@link
 * org.horizon.loader.jdbc.binary.JdbcBinaryCacheStore}, this cache store will store each entry within a row in the
 * table (rather than grouping multiple entries into an row). This assures a finer graned granularity for all operation,
 * and better performance. In order to be able to store non-string keys, it relies on an {@link Key2StringMapper}.
 * <p/>
 * The actual storage table is defined through configuration {@link JdbcStringBasedCacheStore}. The table can be
 * created/dropped on-the-fly, at deployment time. For more details consult javadoc for {@link
 * JdbcStringBasedCacheStore}.
 *
 * @author Mircea.Markus@jboss.com
 * @see Key2StringMapper
 * @see DefaultKey2StringMapper
 */
public class JdbcStringBasedCacheStore extends LockSupportCacheStore {

   private static Log log = LogFactory.getLog(JdbcStringBasedCacheStore.class);

   /** delimits the stram for stream trasfer operations */
   private static final String STRING_STREAM_DELIMITER = "__JdbcCacheStore_done__";

   private JdbcStringBasedCacheStoreConfig config;
   private Key2StringMapper key2StringMapper;
   private ConnectionFactory connectionFactory;
   private TableManipulation tableManipulation;
   public void init(CacheLoaderConfig config, Cache cache, Marshaller m) {
      super.init(config, cache, m);
      this.config = (JdbcStringBasedCacheStoreConfig) config;
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      if (config.isManageConnectionFactory()) {
         String connectionFactoryClass = config.getConnectionFactoryConfig().getConnectionFactoryClass();
         ConnectionFactory connectionFactory = ConnectionFactory.getConnectionFactory(connectionFactoryClass);
         connectionFactory.start(config.getConnectionFactoryConfig());
         doConnectionFactoryInitialization(connectionFactory);
      }
      this.key2StringMapper = config.getKey2StringMapper();
   }

   public void stop() throws CacheLoaderException {
      tableManipulation.stop();
      if (config.isManageConnectionFactory()) {
         connectionFactory.stop();
      }
   }

   protected String getLockFromKey(Object key) throws CacheLoaderException {
      if (!key2StringMapper.isSupportedType(key.getClass())) {
         throw new UnsupportedKeyTypeException(key);
      }
      return key2StringMapper.getStringMapping(key);
   }

   public void storeLockSafe(StoredEntry ed, String lockingKey) throws CacheLoaderException {
      StoredEntry existingOne = loadLockSafe(ed, lockingKey);
      String sql;
      if (existingOne == null) {
         sql = tableManipulation.getInsertRowSql();
      } else {
         sql = tableManipulation.getUpdateRowSql();
      }
      if (log.isTraceEnabled())
         log.trace("Running sql '" + sql + "' on " + ed + ". Key string is '" + lockingKey + "'");
      Connection connection = null;
      PreparedStatement ps = null;
      try {
         connection = connectionFactory.getConnection();
         ps = connection.prepareStatement(sql);
         ByteBuffer byteBuffer = JdbcUtil.marshall(getMarshaller(), ed);
         ps.setBinaryStream(1, byteBuffer.getStream(), byteBuffer.getLength());
         ps.setLong(2, ed.getExpiryTime());
         ps.setString(3, lockingKey);
         ps.executeUpdate();
      } catch (SQLException ex) {
         logAndThrow(ex, "Error while storing string keys to database");
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(connection);
      }
   }

   public boolean removeLockSafe(Object key, String keyStr) throws CacheLoaderException {
      Connection connection = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManipulation.getDeleteRowSql();
         if (log.isTraceEnabled()) log.trace("Running sql '" + sql + "' on " + keyStr);
         connection = connectionFactory.getConnection();
         ps = connection.prepareStatement(sql);
         ps.setString(1, keyStr);
         return ps.executeUpdate() == 1;
      } catch (SQLException ex) {
         String message = "Error while storing string keys to database";
         log.error(message, ex);
         throw new CacheLoaderException(message, ex);
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(connection);
      }
   }

   public void fromStreamLockSafe(ObjectInput objectInput) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         conn = connectionFactory.getConnection();
         String sql = tableManipulation.getInsertRowSql();
         ps = conn.prepareStatement(sql);

         int readStoredEntries = 0;
         int batchSize = config.getBatchSize();
         Object objFromStream = objectInput.readObject();
         while (!objFromStream.equals(STRING_STREAM_DELIMITER)) {
            StoredEntry se = (StoredEntry) objFromStream;
            readStoredEntries++;
            String key = key2StringMapper.getStringMapping(se.getKey());
            ByteBuffer buffer = JdbcUtil.marshall(getMarshaller(), se);
            ps.setBinaryStream(1, buffer.getStream(), buffer.getLength());
            ps.setLong(2, se.getExpiryTime());
            ps.setString(3, key);
            ps.addBatch();
            if (readStoredEntries % batchSize == 0) {
               ps.executeBatch();
               if (log.isTraceEnabled())
                  log.trace("Executing batch " + (readStoredEntries / batchSize) + ", batch size is " + batchSize);
            }
            objFromStream = objectInput.readObject();
         }
         if (readStoredEntries % batchSize != 0)
            ps.executeBatch();//flush the batch
         if (log.isTraceEnabled())
            log.trace("Successfully inserted " + readStoredEntries + " buckets into the database, batch size is " + batchSize);
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
      //now write our data
      Connection connection = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         String sql = tableManipulation.getLoadAllRowsSql();
         if (log.isTraceEnabled()) log.trace("Running sql '" + sql);
         connection = connectionFactory.getConnection();
         ps = connection.prepareStatement(sql);
         rs = ps.executeQuery();
         rs.setFetchSize(config.getFetchSize());
         while (rs.next()) {
            InputStream is = rs.getBinaryStream(1);
            StoredEntry se = (StoredEntry) JdbcUtil.unmarshall(getMarshaller(), is);
            objectOutput.writeObject(se);
         }
         objectOutput.writeObject(STRING_STREAM_DELIMITER);
      } catch (SQLException e) {
         logAndThrow(e, "SQL Error while storing string keys to database");
      } catch (IOException e) {
         logAndThrow(e, "I/O Error while storing string keys to database");
      }
      finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(connection);
      }
   }

   @Override
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

   @Override
   public void purgeInternal() throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManipulation.getDeleteExpiredRowsSql();
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ps.setLong(1, System.currentTimeMillis());
         int result = ps.executeUpdate();
         if (log.isTraceEnabled())
            log.trace("Successfully purged " + result + " rows.");
      } catch (SQLException ex) {
         logAndThrow(ex, "Failed purging JdbcBinaryCacheStore");
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   protected Set<StoredEntry> loadAllLockSafe() throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         String sql = tableManipulation.getLoadAllRowsSql();
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         rs = ps.executeQuery();
         rs.setFetchSize(config.getFetchSize());
         Set<StoredEntry> result = new HashSet<StoredEntry>();
         while (rs.next()) {
            InputStream inputStream = rs.getBinaryStream(1);
            StoredEntry se = (StoredEntry) JdbcUtil.unmarshall(getMarshaller(), inputStream);
            result.add(se);
         }
         return result;
      } catch (SQLException e) {
         String message = "SQL error while fetching all StoredEntries";
         log.error(message, e);
         throw new CacheLoaderException(message, e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   protected StoredEntry loadLockSafe(Object key, String lockingKey) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         String sql = tableManipulation.getSelectRowSql();
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         ps.setString(1, lockingKey);
         rs = ps.executeQuery();
         if (rs.next()) {
            InputStream inputStream = rs.getBinaryStream(2);
            StoredEntry storedEntry = (StoredEntry) JdbcUtil.unmarshall(getMarshaller(), inputStream);
            if (storedEntry.isExpired()) {
               if (log.isTraceEnabled()) {
                  log.trace("Not returning '" + storedEntry + "' as it is expired. It will be removed from DB by purging thread!");
               }
               return null;
            }
            return storedEntry;
         }
         return null;
      } catch (SQLException e) {
         String message = "SQL error while fetching strored entry with key:" + key + " lockingKey: " + lockingKey;
         log.error(message, e);
         throw new CacheLoaderException(message, e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return JdbcStringBasedCacheStoreConfig.class;
   }

   protected void logAndThrow(Exception e, String message) throws CacheLoaderException {
      log.error(message, e);
      throw new CacheLoaderException(message, e);
   }

   public boolean supportsKey(Class keyType) {
      return key2StringMapper.isSupportedType(keyType);
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

   public ConnectionFactory getConnectionFactory() {
      return connectionFactory;
   }
}
