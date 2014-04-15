package org.infinispan.persistence.jdbc.configuration;

import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.XmlConfigHelper;
import org.infinispan.persistence.jdbc.Dialect;
import org.infinispan.persistence.jdbc.TableManipulation;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.Key2StringMapper;

/**
 *
 * JdbcMixedCacheStoreConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class JdbcMixedStoreConfigurationBuilder extends AbstractJdbcStoreConfigurationBuilder<JdbcMixedStoreConfiguration, JdbcMixedStoreConfigurationBuilder>
      implements JdbcMixedStoreConfigurationChildBuilder<JdbcMixedStoreConfigurationBuilder> {
   private final MixedTableManipulationConfigurationBuilder binaryTable;
   private final MixedTableManipulationConfigurationBuilder stringTable;
   private String key2StringMapper = DefaultTwoWayKey2StringMapper.class.getName();
   private int batchSize = TableManipulation.DEFAULT_BATCH_SIZE;
   private int fetchSize = TableManipulation.DEFAULT_FETCH_SIZE;

   private int lockConcurrencyLevel = JdbcBinaryStoreConfigurationBuilder.DEFAULT_CONCURRENCY_LEVEL;
   private long lockAcquisitionTimeout = JdbcBinaryStoreConfigurationBuilder.DEFAULT_LOCK_ACQUISITION_TIMEOUT;


   public JdbcMixedStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
      this.binaryTable = new MixedTableManipulationConfigurationBuilder(this);
      this.stringTable = new MixedTableManipulationConfigurationBuilder(this);
   }

   @Override
   public JdbcMixedStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * When doing repetitive DB inserts this will be batched
    * according to this parameter. This is an optional parameter, and if it is not specified it will
    * be defaulted to {@link org.infinispan.persistence.jdbc.TableManipulation#DEFAULT_BATCH_SIZE}.
    */
   public JdbcMixedStoreConfigurationBuilder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
   }

   /**
    * For DB queries the fetch size will be set on {@link java.sql.ResultSet#setFetchSize(int)}. This is optional
    * parameter, if not specified will be defaulted to {@link org.infinispan.persistence.jdbc.TableManipulation#DEFAULT_FETCH_SIZE}.
    */
   public JdbcMixedStoreConfigurationBuilder fetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
   }

   /**
    * Allows configuration of table-specific parameters such as column names and types for the table
    * used to store entries with binary keys
    */
   @Override
   public MixedTableManipulationConfigurationBuilder binaryTable() {
      return binaryTable;
   }

   /**
    * Allows configuration of table-specific parameters such as column names and types for the table
    * used to store entries with string keys
    */
   @Override
   public MixedTableManipulationConfigurationBuilder stringTable() {
      return stringTable;
   }

   @Override
   public JdbcMixedStoreConfigurationBuilder withProperties(Properties props) {
      Map<Object, Object> unrecognized = XmlConfigHelper.setValues(this, props, false, false);
      XmlConfigHelper.setValues(binaryTable, unrecognized, false, false);
      unrecognized = XmlConfigHelper.setValues(stringTable, unrecognized, false, false);
      XmlConfigHelper.showUnrecognizedAttributes(unrecognized);
      this.properties = props;
      return this;
   }

   /**
    * The class name of a {@link org.infinispan.persistence.keymappers.Key2StringMapper} to use for mapping keys to strings suitable for
    * storage in a database table. Defaults to {@link org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper}
    */
   @Override
   public JdbcMixedStoreConfigurationChildBuilder<JdbcMixedStoreConfigurationBuilder> key2StringMapper(String key2StringMapper) {
      this.key2StringMapper = key2StringMapper;
      return this;
   }

   /**
    * The class of a {@link org.infinispan.persistence.keymappers.Key2StringMapper} to use for mapping keys to strings suitable for
    * storage in a database table. Defaults to {@link org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper}
    */
   @Override
   public JdbcMixedStoreConfigurationChildBuilder<JdbcMixedStoreConfigurationBuilder> key2StringMapper(Class<? extends Key2StringMapper> klass) {
      this.key2StringMapper = klass.getName();
      return this;
   }

   @Override
   public void validate() {
      super.validate();
      if (binaryTable.tableNamePrefix.equals(stringTable.tableNamePrefix))
         throw new CacheConfigurationException("There cannot be the same tableNamePrefix on both the binary and " +
               "String tables.");

   }

   public JdbcMixedStoreConfigurationBuilder lockConcurrencyLevel(int l) {
      this.lockConcurrencyLevel = l;
      return this;
   }

   public JdbcMixedStoreConfigurationBuilder lockAcquisitionTimeout(long timeout) {
      this.lockAcquisitionTimeout = timeout;
      return this;
   }

   @Override
   public JdbcMixedStoreConfiguration create() {
      return new JdbcMixedStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                             singletonStore.create(), preload, shared, properties, connectionFactory.create(), manageConnectionFactory,
                                             batchSize, fetchSize, dialect, binaryTable.create(), stringTable.create(), key2StringMapper, lockConcurrencyLevel, lockAcquisitionTimeout);
   }

   @Override
   public JdbcMixedStoreConfigurationBuilder read(JdbcMixedStoreConfiguration template) {
      super.read(template);

      this.binaryTable.read(template.binaryTable());
      this.stringTable.read(template.stringTable());
      this.key2StringMapper = template.key2StringMapper();
      this.batchSize = template.batchSize();
      this.fetchSize = template.fetchSize();
      this.lockAcquisitionTimeout = template.lockAcquisitionTimeout();
      this.lockConcurrencyLevel = template.lockConcurrencyLevel();
      return this;
   }

   public class MixedTableManipulationConfigurationBuilder extends
         TableManipulationConfigurationBuilder<JdbcMixedStoreConfigurationBuilder, MixedTableManipulationConfigurationBuilder> implements
                                                                                                                               JdbcMixedStoreConfigurationChildBuilder<JdbcMixedStoreConfigurationBuilder> {

      MixedTableManipulationConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, JdbcMixedStoreConfigurationBuilder> builder) {
         super(builder);
      }

      @Override
      public MixedTableManipulationConfigurationBuilder self() {
         return this;
      }

      @Override
      public MixedTableManipulationConfigurationBuilder binaryTable() {
         return binaryTable;
      }

      @Override
      public MixedTableManipulationConfigurationBuilder stringTable() {
         return stringTable;
      }

      @Override
      public PooledConnectionFactoryConfigurationBuilder<JdbcMixedStoreConfigurationBuilder> connectionPool() {
         return JdbcMixedStoreConfigurationBuilder.this.connectionPool();
      }

      @Override
      public ManagedConnectionFactoryConfigurationBuilder<JdbcMixedStoreConfigurationBuilder> dataSource() {
         return JdbcMixedStoreConfigurationBuilder.this.dataSource();
      }

      @Override
      public JdbcMixedStoreConfigurationChildBuilder<JdbcMixedStoreConfigurationBuilder> key2StringMapper(String key2StringMapper) {
         return JdbcMixedStoreConfigurationBuilder.this.key2StringMapper(key2StringMapper);
      }

      @Override
      public JdbcMixedStoreConfigurationChildBuilder<JdbcMixedStoreConfigurationBuilder> key2StringMapper(Class<? extends Key2StringMapper> klass) {
         return JdbcMixedStoreConfigurationBuilder.this.key2StringMapper(klass);
      }
   }
}
