package org.infinispan.tools.store.migrator.jdbc;

import static org.infinispan.tools.store.migrator.Element.BINARY;
import static org.infinispan.tools.store.migrator.Element.SOURCE;

import java.util.Iterator;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.connectionfactory.PooledConnectionFactory;
import org.infinispan.persistence.jdbc.table.management.DbMetaData;
import org.infinispan.persistence.jdbc.table.management.TableManager;
import org.infinispan.persistence.jdbc.table.management.TableManagerFactory;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.tools.store.migrator.Element;
import org.infinispan.tools.store.migrator.StoreIterator;
import org.infinispan.tools.store.migrator.StoreProperties;
import org.infinispan.tools.store.migrator.StoreType;
import org.infinispan.tools.store.migrator.marshaller.SerializationConfigUtil;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
public class JdbcStoreReader implements StoreIterator {

   private final StoreProperties props;
   private final StreamingMarshaller marshaller;
   private final JdbcStringBasedStoreConfiguration config;
   private final ConnectionFactory connectionFactory;
   private final DbMetaData metaData;
   private final TableManipulationConfiguration stringConfig;
   private final TableManipulationConfiguration binaryConfig;

   public JdbcStoreReader(StoreProperties props) {
      this.props = props;
      this.marshaller = SerializationConfigUtil.getMarshaller(props);
      this.config = JdbcConfigurationUtil.getStoreConfig(props);
      this.connectionFactory = new PooledConnectionFactory();
      String segmentCount = props.get(Element.SEGMENT_COUNT);
      this.metaData = new DbMetaData(config.dialect(), config.dbMajorVersion(), config.dbMinorVersion(), false, false,
            // If we don't have segments then disable it
            segmentCount == null || Integer.parseInt(segmentCount) <= 0);
      this.stringConfig = config.table();
      this.binaryConfig = createBinaryTableConfig();

      connectionFactory.start(config.connectionFactory(), JdbcStoreReader.class.getClassLoader());
   }

   @Override
   public void close() {
      connectionFactory.stop();
   }

   public Iterator<MarshalledEntry> iterator() {
      switch (props.storeType()) {
         case JDBC_BINARY:
            return new BinaryJdbcIterator(connectionFactory, getTableManager(true), marshaller);
         case JDBC_STRING:
            return new StringJdbcIterator(connectionFactory, getTableManager(false), marshaller, getTwoWayMapper());
         case JDBC_MIXED:
            return new MixedJdbcIterator(connectionFactory, getTableManager(true), getTableManager(false),
                  marshaller, getTwoWayMapper());
         default:
            throw new CacheConfigurationException("Unknown Store Type: " + props.storeType());
      }
   }

   private TableManager getTableManager(boolean binary) {
      TableManipulationConfiguration tableConfig = binary ? binaryConfig : stringConfig;
      TableManager tableManager = TableManagerFactory.getManager(metaData, connectionFactory, tableConfig);
      tableManager.setCacheName(props.cacheName());
      return tableManager;
   }

   private TableManipulationConfiguration createBinaryTableConfig() {
      if (props.storeType() == StoreType.JDBC_STRING)
         return null;

      JdbcStringBasedStoreConfigurationBuilder builder = new ConfigurationBuilder().persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      return JdbcConfigurationUtil.createTableConfig(props, BINARY, builder);
   }

   private TwoWayKey2StringMapper getTwoWayMapper() {
      String mapperClass = config.key2StringMapper();
      if (mapperClass != null) {
         ClassLoader classLoader = JdbcConfigurationUtil.class.getClassLoader();
         try {
            return (TwoWayKey2StringMapper) Util.loadClass(mapperClass, classLoader).newInstance();
         } catch (IllegalAccessException | InstantiationException e) {
            throw new CacheConfigurationException(String.format("Unabled to load TwoWayKey2StringMapper '%s' for %s store",
                  mapperClass, SOURCE), e);
         }
      }
      return new DefaultTwoWayKey2StringMapper();
   }
}
