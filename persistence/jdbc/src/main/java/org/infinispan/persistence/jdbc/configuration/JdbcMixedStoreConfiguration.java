package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.jdbc.TableManipulation;
import org.infinispan.persistence.jdbc.mixed.JdbcMixedStore;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;

/**
 *
 * JdbcMixedStoreConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(JdbcMixedStoreConfigurationBuilder.class)
@ConfigurationFor(JdbcMixedStore.class)
public class JdbcMixedStoreConfiguration extends AbstractJdbcStoreConfiguration {
   static final AttributeDefinition<Integer> BATCH_SIZE = AttributeDefinition.builder("batchSize", TableManipulation.DEFAULT_BATCH_SIZE).immutable().build();
   static final AttributeDefinition<Integer> FETCH_SIZE = AttributeDefinition.builder("fetchSize", TableManipulation.DEFAULT_FETCH_SIZE).immutable().build();
   static final AttributeDefinition<String> KEY2STRING_MAPPER = AttributeDefinition.builder("key2StringMapper", DefaultTwoWayKey2StringMapper.class.getName()).immutable().build();
   static final AttributeDefinition<Integer> CONCURRENCY_LEVEL = AttributeDefinition.builder("concurrencyLevel", 2048).immutable().build();
   static final AttributeDefinition<Long> LOCK_ACQUISITION_TIMEOUT = AttributeDefinition.builder("lockAcquisitionTimeout", 60000l).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(JdbcMixedStoreConfiguration.class, AbstractJdbcStoreConfiguration.attributeDefinitionSet(), BATCH_SIZE, FETCH_SIZE, KEY2STRING_MAPPER, CONCURRENCY_LEVEL, LOCK_ACQUISITION_TIMEOUT);
   }


   private final Attribute<Integer> batchSize;
   private final Attribute<Integer> fetchSize;
   private final Attribute<String> key2StringMapper;
   private final Attribute<Integer> concurrencyLevel;
   private final Attribute<Long> lockAcquisitionTimeout;
   private final TableManipulationConfiguration binaryTable;
   private final TableManipulationConfiguration stringTable;

   public JdbcMixedStoreConfiguration(AttributeSet attributes,
                                      AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                      ConnectionFactoryConfiguration connectionFactory,
                                      TableManipulationConfiguration binaryTable,
                                      TableManipulationConfiguration stringTable) {
      super(attributes, async, singletonStore, connectionFactory);
      this.binaryTable = binaryTable;
      this.stringTable = stringTable;
      batchSize = attributes.attribute(BATCH_SIZE);
      fetchSize = attributes.attribute(FETCH_SIZE);
      key2StringMapper = attributes.attribute(KEY2STRING_MAPPER);
      concurrencyLevel = attributes.attribute(CONCURRENCY_LEVEL);
      lockAcquisitionTimeout = attributes.attribute(LOCK_ACQUISITION_TIMEOUT);
   }

   public String key2StringMapper() {
      return key2StringMapper.get();
   }

   public TableManipulationConfiguration binaryTable() {
      return binaryTable;
   }

   public TableManipulationConfiguration stringTable() {
      return stringTable;
   }

   public int batchSize() {
      return batchSize.get();
   }

   public int fetchSize() {
      return fetchSize.get();
   }

   public int lockConcurrencyLevel() {
      return concurrencyLevel.get();
   }

   public long lockAcquisitionTimeout() {
      return lockAcquisitionTimeout.get();
   }

   @Override
   public String toString() {
      return "JdbcMixedStoreConfiguration [binaryTable=" + binaryTable + ", stringTable=" + stringTable + ", attributes=" + attributes + ", connectionFactory()="
            + connectionFactory() + ", async()=" + async() + ", singletonStore()=" + singletonStore() + "]";
   }
}