package org.infinispan.persistence.jdbc.configuration;

import java.util.Map;
import java.util.Properties;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.XmlConfigHelper;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.Key2StringMapper;
import org.infinispan.commons.configuration.Builder;

/**
 *
 * JdbcStringBasedStoreConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class JdbcStringBasedStoreConfigurationBuilder extends AbstractJdbcStoreConfigurationBuilder<JdbcStringBasedStoreConfiguration, JdbcStringBasedStoreConfigurationBuilder> {
   private String key2StringMapper = DefaultTwoWayKey2StringMapper.class.getName();
   private StringTableManipulationConfigurationBuilder table;

   public JdbcStringBasedStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
      table = new StringTableManipulationConfigurationBuilder(this);
   }

   @Override
   public JdbcStringBasedStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * The class name of a {@link Key2StringMapper} to use for mapping keys to strings suitable for
    * storage in a database table. Defaults to {@link DefaultTwoWayKey2StringMapper}
    */
   public JdbcStringBasedStoreConfigurationBuilder key2StringMapper(String key2StringMapper) {
      this.key2StringMapper = key2StringMapper;
      return this;
   }

   /**
    * The class of a {@link Key2StringMapper} to use for mapping keys to strings suitable for
    * storage in a database table. Defaults to {@link DefaultTwoWayKey2StringMapper}
    */
   public JdbcStringBasedStoreConfigurationBuilder key2StringMapper(Class<? extends Key2StringMapper> klass) {
      this.key2StringMapper = klass.getName();
      return this;
   }

   /**
    * Allows configuration of table-specific parameters such as column names and types
    */
   public StringTableManipulationConfigurationBuilder table() {
      return table;
   }

   @Override
   public JdbcStringBasedStoreConfigurationBuilder withProperties(Properties props) {
      Map<Object, Object> unrecognized = XmlConfigHelper.setValues(this, props, false, false);
      unrecognized = XmlConfigHelper.setValues(table, unrecognized, false, false);
      XmlConfigHelper.showUnrecognizedAttributes(unrecognized);
      this.properties = props;
      return this;
   }

   @Override
   public JdbcStringBasedStoreConfiguration create() {
      ConnectionFactoryConfiguration cf = connectionFactory != null ? connectionFactory.create() : null;
      return new JdbcStringBasedStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                                   singletonStore.create(), preload, shared, properties, cf, manageConnectionFactory, key2StringMapper, table.create(), databaseType);
   }

   @Override
   public Builder<?> read(JdbcStringBasedStoreConfiguration template) {
      super.read(template);
      this.key2StringMapper = template.key2StringMapper();
      this.table.read(template.table());

      return this;
   }

   public class StringTableManipulationConfigurationBuilder extends TableManipulationConfigurationBuilder<JdbcStringBasedStoreConfigurationBuilder, StringTableManipulationConfigurationBuilder> {

      StringTableManipulationConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, JdbcStringBasedStoreConfigurationBuilder> builder) {
         super(builder);
      }

      @Override
      public StringTableManipulationConfigurationBuilder self() {
         return this;
      }

      @Override
      public PooledConnectionFactoryConfigurationBuilder<JdbcStringBasedStoreConfigurationBuilder> connectionPool() {
         return JdbcStringBasedStoreConfigurationBuilder.this.connectionPool();
      }

      @Override
      public ManagedConnectionFactoryConfigurationBuilder<JdbcStringBasedStoreConfigurationBuilder> dataSource() {
         return JdbcStringBasedStoreConfigurationBuilder.this.dataSource();
      }
   }

}
