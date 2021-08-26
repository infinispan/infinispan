package org.infinispan.persistence.jdbc.stringbased;

import static org.infinispan.persistence.jdbc.common.JdbcUtil.marshall;
import static org.infinispan.persistence.jdbc.common.JdbcUtil.unmarshall;
import static org.infinispan.persistence.jdbc.common.logging.Log.PERSISTENCE;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.jdbc.common.JdbcUtil;
import org.infinispan.persistence.jdbc.common.TableOperations;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.BaseJdbcStore;
import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.jdbc.impl.table.TableManagerFactory;
import org.infinispan.persistence.keymappers.Key2StringMapper;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.MarshalledValue;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.UnicastProcessor;

/**
 * {@link org.infinispan.persistence.spi.AdvancedCacheLoader} implementation that stores the entries in a database.
 * This cache store will store each entry within a row in the table. This assures a finer grained granularity for all
 * operation, and better performance. In order to be able to store non-string keys, it relies on an {@link
 * org.infinispan.persistence.keymappers.Key2StringMapper}.
 * <p/>
 * Note that only the keys are stored as strings, the values are still saved as binary data. Using a character
 * data type for the value column will result in unmarshalling errors.
 * <p/>
 * The actual storage table is defined through configuration {@link org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration}.
 * The table can be created/dropped on-the-fly, at deployment time. For more details consult javadoc for {@link
 * org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration}.
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
@ConfiguredBy(JdbcStringBasedStoreConfiguration.class)
public class JdbcStringBasedStore<K, V> extends BaseJdbcStore<K, V, JdbcStringBasedStoreConfiguration> {

   private static final Log log = LogFactory.getLog(JdbcStringBasedStore.class, Log.class);

   private JdbcStringBasedStoreConfiguration configuration;

   private Key2StringMapper key2StringMapper;
   private MarshallableEntryFactory<K, V> marshalledEntryFactory;
   private PersistenceMarshaller marshaller;
   private TimeService timeService;
   private KeyPartitioner keyPartitioner;

   @Override
   public Set<Characteristic> characteristics() {
      return EnumSet.of(Characteristic.BULK_READ, Characteristic.EXPIRATION, Characteristic.SEGMENTABLE,
            Characteristic.TRANSACTIONAL, Characteristic.SHAREABLE);
   }

   @Override
   protected TableOperations<K, V> createTableOperations(InitializationContext ctx, JdbcStringBasedStoreConfiguration configuration) {
      this.configuration = ctx.getConfiguration();
      this.marshalledEntryFactory = ctx.getMarshallableEntryFactory();
      this.marshaller = ctx.getPersistenceMarshaller();
      this.timeService = ctx.getTimeService();
      this.keyPartitioner = configuration.segmented() ? ctx.getKeyPartitioner() : null;

      String cacheName = ctx.getCache().getName();
      TableManager<K, V> tableManager = TableManagerFactory.getManager(ctx, connectionFactory, configuration,
            ctx.getCache().getName());
      tableManager.start();

      if (!configuration.table().createOnStart()) {
         Connection connection = null;
         try {
            connection = connectionFactory.getConnection();
            // If meta exists, then ensure that the stored configuration is compatible with the current settings
            if (tableManager.metaTableExists(connection)) {
               TableManager.Metadata meta = tableManager.getMetadata(connection);
               int storedSegments = meta.getSegments();
               if (!configuration.segmented()) {
                  // ISPN-13135 number of segments was previously written incorrectly, so don't validate number for older versions
                  String versionStr = Version.decodeVersion(meta.getVersion());
                  List<Integer> versionParts = Arrays.stream(versionStr.split("\\.")).map(Integer::parseInt).collect(Collectors.toList());
                  // Ignore check if version < 12.1.5. Meta table only created since 12.0.0
                  if ((versionParts.get(0) > 12 || versionParts.get(2) > 4) && storedSegments != -1)
                     throw log.existingStoreNoSegmentation();
               }

               int configuredSegments = ctx.getCache().getCacheConfiguration().clustering().hash().numSegments();
               if (configuration.segmented() && storedSegments != configuredSegments)
                  throw log.existingStoreSegmentMismatch(storedSegments, configuredSegments);
               tableManager.updateMetaTable(connection);
            } else {
               // The meta table does not exist, therefore we must be reading from a 11.x store. Migrate the old data
               org.infinispan.util.logging.Log.PERSISTENCE.startMigratingPersistenceData(cacheName);
               try {
                  migrateFromV11(ctx, tableManager);
               } catch (SQLException e) {
                  throw org.infinispan.util.logging.Log.PERSISTENCE.persistedDataMigrationFailed(cacheName, e);
               }
               tableManager.createMetaTable(connection);
               org.infinispan.util.logging.Log.PERSISTENCE.persistedDataSuccessfulMigrated(cacheName);
            }
         } finally {
            connectionFactory.releaseConnection(connection);
         }
      }

      try {
         Object mapper = Util.loadClassStrict(configuration.key2StringMapper(),
               ctx.getGlobalConfiguration().classLoader()).newInstance();
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
      if (ctx.getCache().getCacheConfiguration() != null && ctx.getCache().getCacheConfiguration().clustering().cacheMode().isDistributed()) {
         enforceTwoWayMapper("distribution/rehashing");
      }

      return tableManager;
   }

   public TableManager<K, V> getTableManager() {
      return (TableManager<K, V>) tableOperations;
   }

   private void migrateFromV11(InitializationContext ctx, TableManager<K, V> tableManager) throws SQLException {
      // If a custom user marshaller was previously used, no need to update rows
      if (ctx.getGlobalConfiguration().serialization().marshaller() != null)
         return;

      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         conn = connectionFactory.getConnection();
         conn.setAutoCommit(false);
         String sql = tableManager.getLoadNonExpiredAllRowsSql();
         ps = conn.prepareStatement(sql);
         ps.setLong(1, timeService.wallClockTime());
         rs = ps.executeQuery();

         Marshaller userMarshaller = marshaller.getUserMarshaller();
         try (PreparedStatement upsertBatch = conn.prepareStatement(tableManager.getUpdateRowSql())) {
            int batchSize = 0;
            while (rs.next()) {
               batchSize++;
               InputStream inputStream = rs.getBinaryStream(1);
               String keyStr = rs.getString(2);
               long timestamp = rs.getLong(3);
               int segment = keyPartitioner == null ? -1 : rs.getInt(4);

               MarshalledValue mv = unmarshall(inputStream, marshaller);
               V value = unmarshall(mv.getValueBytes(), userMarshaller);
               Metadata meta;
               try {
                  meta = unmarshall(mv.getMetadataBytes(), userMarshaller);
               } catch (IllegalArgumentException e) {
                  // For metadata we need to attempt to read with user-marshaller first in case custom metadata used, otherwise use the persistence marshaller
                  meta = unmarshall(mv.getMetadataBytes(), marshaller);
               }

               PrivateMetadata internalMeta = unmarshall(mv.getInternalMetadataBytes(), marshaller);
               MarshallableEntry<K, V> entry = marshalledEntryFactory.create(null, value, meta, internalMeta, mv.getCreated(), mv.getLastUsed());
               ByteBuffer byteBuffer = marshall(entry.getMarshalledValue(), marshaller);
               tableManager.prepareUpdateStatement(upsertBatch, keyStr, timestamp, segment, byteBuffer);
               upsertBatch.addBatch();

               if (batchSize == configuration.maxBatchSize()) {
                  batchSize = 0;
                  upsertBatch.executeBatch();
                  upsertBatch.clearBatch();
               }
            }
            if (batchSize != 0)
               upsertBatch.executeBatch();

            conn.commit();
         }
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   @Override
   protected void extraStopSteps() {
      try {
         TableManager<K, V> tableManager = getTableManager();
         if (tableManager != null) {
            tableManager.stop();
            tableOperations = null;
         }
      } catch (Throwable t) {
         log.debug("Exception while stopping", t);
      }
   }

   @Override
   public CompletionStage<Void> addSegments(IntSet segments) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> removeSegments(IntSet segments) {
      return CompletableFutures.completedNull();
   }

   public ConnectionFactory getConnectionFactory() {
      return connectionFactory;
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> purgeExpired() {
      return Flowable.defer(() -> {
         UnicastProcessor<MarshallableEntry<K, V>> unicastProcessor = UnicastProcessor.create();
         blockingManager.runBlocking(() -> {
            TableManager<K, V> tableManager = getTableManager();
            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
               String sql = tableManager.getSelectOnlyExpiredRowsSql();
               conn = connectionFactory.getConnection();
               conn.setAutoCommit(false);
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

                     if (twoWayMapperExists) {
                        InputStream inputStream = rs.getBinaryStream(1);
                        Object key = ((TwoWayKey2StringMapper) key2StringMapper).getKeyMapping(keyStr);
                        MarshalledValue value = unmarshall(inputStream, marshaller);
                        unicastProcessor.onNext(marshalledEntryFactory.create(key, value));
                     }
                  }

                  if (!twoWayMapperExists)
                     PERSISTENCE.twoWayKey2StringMapperIsMissing(TwoWayKey2StringMapper.class.getSimpleName());

                  if (affectedRows > 0) {
                     int[] result = batchDelete.executeBatch();
                     if (log.isTraceEnabled()) {
                        log.tracef("Successfully purged %d rows.", result.length);
                     }
                  }
                  conn.commit();
                  unicastProcessor.onComplete();
               }
            } catch (SQLException e) {
               log.failedClearingJdbcCacheStore(e);
               try {
                  conn.rollback();
               } catch (SQLException ex) {
                  log.sqlFailureTxRollback(ex);
               }
               unicastProcessor.onError(e);
            } finally {
               JdbcUtil.safeClose(rs);
               JdbcUtil.safeClose(ps);
               connectionFactory.releaseConnection(conn);
            }
         }, "jdbcstringstore-purge");
         return unicastProcessor;
      });
   }

   private void enforceTwoWayMapper(String where) throws PersistenceException {
      if (!(key2StringMapper instanceof TwoWayKey2StringMapper)) {
         PERSISTENCE.invalidKey2StringMapper(where, key2StringMapper.getClass().getName());
         throw new PersistenceException(String.format("Invalid key to string mapper : %s", key2StringMapper.getClass().getName()));
      }
   }
}
