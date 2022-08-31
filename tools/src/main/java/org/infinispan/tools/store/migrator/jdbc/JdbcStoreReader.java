package org.infinispan.tools.store.migrator.jdbc;

import static org.infinispan.tools.store.migrator.Element.BINARY;
import static org.infinispan.tools.store.migrator.Element.SOURCE;

import java.util.Iterator;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.PooledConnectionFactory;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.impl.table.DbMetaData;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.jdbc.impl.table.TableManagerFactory;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.spi.MarshallableEntry;
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
   private final Marshaller marshaller;
   private final JdbcStringBasedStoreConfiguration config;
   private final ConnectionFactory connectionFactory;
   private final DbMetaData metaData;
   private final JdbcStringBasedStoreConfiguration stringConfig;
   private final JdbcStringBasedStoreConfiguration binaryConfig;

   public JdbcStoreReader(StoreProperties props) {
      this.props = props;
      this.marshaller = SerializationConfigUtil.getMarshaller(props);
      this.config = JdbcConfigurationUtil.getStoreConfig(props);
      this.connectionFactory = new PooledConnectionFactory();
      this.stringConfig = config;
      this.binaryConfig = createBinaryTableConfig();

      connectionFactory.start(config.connectionFactory(), JdbcStoreReader.class.getClassLoader());

      String segmentCount = props.get(Element.SEGMENT_COUNT);
      metaData = TableManagerFactory.getDbMetaData(connectionFactory, config,
            // If we don't have segments then disable it
            segmentCount == null || Integer.parseInt(segmentCount) <= 0);
   }

   @Override
   public void close() {
      connectionFactory.stop();
   }

   public Iterator<MarshallableEntry> iterator() {
      switch (props.storeType()) {
         case JDBC_BINARY:
            return new BinaryJdbcIterator(connectionFactory, getTableManager(true), marshaller);
         case JDBC_STRING:
            return props.getMajorVersion() > 9 ?
                  new StringJdbcIterator10(connectionFactory, getTableManager(false), marshaller, getTwoWayMapper()) :
                  new StringJdbcIterator(connectionFactory, getTableManager(false), marshaller, getTwoWayMapper());
         case JDBC_MIXED:
            return new MixedJdbcIterator(connectionFactory, getTableManager(true), getTableManager(false),
                  marshaller, getTwoWayMapper());
         default:
            throw new CacheConfigurationException("Unknown Store Type: " + props.storeType());
      }
   }

   private TableManager getTableManager(boolean binary) {
      JdbcStringBasedStoreConfiguration config = binary ? binaryConfig : stringConfig;
      return TableManagerFactory.getManager(metaData, null, connectionFactory, config, props.cacheName());
   }

   private JdbcStringBasedStoreConfiguration createBinaryTableConfig() {
      if (props.storeType() == StoreType.JDBC_STRING)
         return null;

      JdbcStringBasedStoreConfigurationBuilder builder = new ConfigurationBuilder().persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      JdbcConfigurationUtil.createTableConfig(props, BINARY, builder);
      return builder.create();
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
