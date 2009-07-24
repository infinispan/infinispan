package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.io.ByteBuffer;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.LockSupportCacheStore;
import org.infinispan.loaders.jdbc.DataManiulationHelper;
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
import java.util.Set;

/**
 * {@link org.infinispan.loaders.CacheStore} implementation that stores the entries in a database. In contrast to the
 * {@link org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore}, this cache store will store each entry within a row
 * in the table (rather than grouping multiple entries into an row). This assures a finer graned granularity for all
 * operation, and better performance. In order to be able to store non-string keys, it relies on an {@link
 * Key2StringMapper}.
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

   /**
    * delimits the stram for stream trasfer operations
    */
   private static final byte STRING_STREAM_DELIMITER = 100;

   private JdbcStringBasedCacheStoreConfig config;
   private Key2StringMapper key2StringMapper;
   private ConnectionFactory connectionFactory;
   private TableManipulation tableManipulation;
   private DataManiulationHelper dmHelper;

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
      dmHelper = new DataManiulationHelper(connectionFactory, tableManipulation, marshaller) {

         @Override
         public void loadAllProcess(ResultSet rs, Set<InternalCacheEntry> result) throws SQLException, CacheLoaderException {
            InputStream inputStream = rs.getBinaryStream(1);
            InternalCacheValue icv = (InternalCacheValue) JdbcUtil.unmarshall(getMarshaller(), inputStream);
            Object key = rs.getObject(2);
            result.add(icv.toInternalCacheEntry(key));
         }

         @Override
         public void toStreamProcess(ResultSet rs, InputStream is, ObjectOutput objectOutput) throws CacheLoaderException, SQLException, IOException {
            InternalCacheValue icv = (InternalCacheValue) JdbcUtil.unmarshall(getMarshaller(), is);
            Object key = rs.getObject(2);
            marshaller.objectToObjectStream(icv.toInternalCacheEntry(key), objectOutput);
         }

         public boolean fromStreamProcess(Object objFromStream, PreparedStatement ps, ObjectInput objectInput) throws SQLException, CacheLoaderException {
            if (objFromStream instanceof InternalCacheEntry) {
               InternalCacheEntry se = (InternalCacheEntry) objFromStream;
               String key = key2StringMapper.getStringMapping(se.getKey());
               ByteBuffer buffer = JdbcUtil.marshall(getMarshaller(), se.toInternalCacheValue());
               ps.setBinaryStream(1, buffer.getStream(), buffer.getLength());
               ps.setLong(2, se.getExpiryTime());
               ps.setString(3, key);
               return true;
            } else {
               return false;
            }
         }
      };
   }

   @Override
   public void stop() throws CacheLoaderException {
      tableManipulation.stop();
      if (config.isManageConnectionFactory()) {
         connectionFactory.stop();
      }
   }

   @Override
   protected String getLockFromKey(Object key) throws CacheLoaderException {
      if (!key2StringMapper.isSupportedType(key.getClass())) {
         throw new UnsupportedKeyTypeException(key);
      }
      return key2StringMapper.getStringMapping(key);
   }

   @Override
   public void storeLockSafe(InternalCacheEntry ed, String lockingKey) throws CacheLoaderException {
      InternalCacheEntry existingOne = loadLockSafe(ed, lockingKey);
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
         ByteBuffer byteBuffer = JdbcUtil.marshall(getMarshaller(), ed.toInternalCacheValue());
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

   @Override
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

   @Override
   public void fromStreamLockSafe(ObjectInput objectInput) throws CacheLoaderException {
      dmHelper.fromStreamSupport(objectInput);
   }

   @Override
   protected void toStreamLockSafe(ObjectOutput objectOutput) throws CacheLoaderException {
      dmHelper.toStreamSupport(objectOutput, STRING_STREAM_DELIMITER);
   }

   @Override
   protected void clearLockSafe() throws CacheLoaderException {
      dmHelper.clear();
   }

   @Override
   protected Set<InternalCacheEntry> loadAllLockSafe() throws CacheLoaderException {
      return dmHelper.loadAllSupport();
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


   @Override
   protected InternalCacheEntry loadLockSafe(Object key, String lockingKey) throws CacheLoaderException {
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
            InternalCacheValue icv = (InternalCacheValue) JdbcUtil.unmarshall(getMarshaller(), inputStream);
            InternalCacheEntry storedEntry = icv.toInternalCacheEntry(key);
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
    * org.infinispan.loaders.jdbc.TableManipulation} that needs connections. This method should be called when you don't
    * want the store to manage the connection factory, perhaps because it is using an shared connection factory: see
    * {@link org.infinispan.loaders.jdbc.mixed.JdbcMixedCacheStore} for such an example of this.
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
