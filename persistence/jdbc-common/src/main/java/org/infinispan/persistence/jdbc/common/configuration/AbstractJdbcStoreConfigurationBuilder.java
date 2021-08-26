package org.infinispan.persistence.jdbc.common.configuration;

import static org.infinispan.persistence.jdbc.common.logging.Log.PERSISTENCE;

import java.lang.reflect.Constructor;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationUtils;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.jdbc.common.DatabaseType;

public abstract class AbstractJdbcStoreConfigurationBuilder<T extends AbstractJdbcStoreConfiguration, S extends AbstractJdbcStoreConfigurationBuilder<T, S>> extends
      AbstractStoreConfigurationBuilder<T, S> implements JdbcStoreConfigurationChildBuilder<S> {
   protected ConnectionFactoryConfigurationBuilder<ConnectionFactoryConfiguration> connectionFactory;

   public AbstractJdbcStoreConfigurationBuilder(PersistenceConfigurationBuilder builder, AttributeSet attributes) {
      super(builder, attributes);
   }

   @Override
   public PooledConnectionFactoryConfigurationBuilder<S> connectionPool() {
      return connectionFactory(PooledConnectionFactoryConfigurationBuilder.class);
   }

   @Override
   public ManagedConnectionFactoryConfigurationBuilder<S> dataSource() {
      return connectionFactory(ManagedConnectionFactoryConfigurationBuilder.class);
   }

   @Override
   public SimpleConnectionFactoryConfigurationBuilder<S> simpleConnection() {
      return connectionFactory(SimpleConnectionFactoryConfigurationBuilder.class);
   }

   /**
    * Use the specified ConnectionFactory to handle connection to the database
    */
   @Override
   public <C extends ConnectionFactoryConfigurationBuilder<?>> C connectionFactory(Class<C> klass) {
      if (connectionFactory != null) {
         throw new IllegalStateException("A ConnectionFactory has already been configured for this store");
      }
      try {
         Constructor<C> constructor = klass.getDeclaredConstructor(AbstractJdbcStoreConfigurationBuilder.class);
         C builder = constructor.newInstance(this);
         this.connectionFactory = (ConnectionFactoryConfigurationBuilder<ConnectionFactoryConfiguration>) builder;
         return builder;
      } catch (Exception e) {
         throw new CacheConfigurationException("Could not instantiate loader configuration builder '" + klass.getName() + "'", e);
      }
   }

   /**
    * Use the specified {@link ConnectionFactoryConfigurationBuilder} to configure connections to
    * the database
    */
   @Override
   public <C extends ConnectionFactoryConfigurationBuilder<?>> C connectionFactory(C builder) {
      if (connectionFactory != null) {
         throw new IllegalStateException("A ConnectionFactory has already been configured for this store");
      }
      this.connectionFactory = (ConnectionFactoryConfigurationBuilder<ConnectionFactoryConfiguration>) builder;
      return builder;
   }

   public ConnectionFactoryConfigurationBuilder<ConnectionFactoryConfiguration> getConnectionFactory() {
      return connectionFactory;
   }

   /**
    * @param manageConnectionFactory ignored
    * @return this
    * @deprecated Deprecated since 13.0 with no replacement
    */
   public S manageConnectionFactory(boolean manageConnectionFactory) {
      return self();
   }

   public S dialect(DatabaseType databaseType) {
      attributes.attribute(AbstractJdbcStoreConfiguration.DIALECT).set(databaseType);
      return self();
   }

   public S dbMajorVersion(Integer majorVersion) {
      attributes.attribute(AbstractJdbcStoreConfiguration.DB_MAJOR_VERSION).set(majorVersion);
      return self();
   }

   public S dbMinorVersion(Integer minorVersion) {
      attributes.attribute(AbstractJdbcStoreConfiguration.DB_MINOR_VERSION).set(minorVersion);
      return self();
   }

   public S readQueryTimeout(Integer queryTimeout) {
      attributes.attribute(AbstractJdbcStoreConfiguration.READ_QUERY_TIMEOUT).set(queryTimeout);
      return self();
   }

   public S writeQueryTimeout(Integer queryTimeout) {
      attributes.attribute(AbstractJdbcStoreConfiguration.WRITE_QUERY_TIMEOUT).set(queryTimeout);
      return self();
   }

   @Override
   public void validate() {
      super.validate();
      if (connectionFactory == null) {
         throw PERSISTENCE.missingConnectionFactory();
      }

      connectionFactory.validate();
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      // Can't validate global config with connection factory
   }

   @Override
   public Builder<?> read(T template) {
      super.read(template);
      Class<? extends ConnectionFactoryConfigurationBuilder<?>> cfb = (Class<? extends ConnectionFactoryConfigurationBuilder<?>>) ConfigurationUtils.builderFor(template
            .connectionFactory());
      connectionFactory(cfb);
      connectionFactory.read(template.connectionFactory());

      return this;
   }
}
