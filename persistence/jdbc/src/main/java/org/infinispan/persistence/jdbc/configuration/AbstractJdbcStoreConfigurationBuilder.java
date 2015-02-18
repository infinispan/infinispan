package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.AbstractJdbcStoreConfiguration.DIALECT;
import static org.infinispan.persistence.jdbc.configuration.AbstractJdbcStoreConfiguration.MANAGE_CONNECTION_FACTORY;

import java.lang.reflect.Constructor;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationUtils;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;

public abstract class AbstractJdbcStoreConfigurationBuilder<T extends AbstractJdbcStoreConfiguration, S extends AbstractJdbcStoreConfigurationBuilder<T, S>> extends
      AbstractStoreConfigurationBuilder<T, S> implements JdbcStoreConfigurationChildBuilder<S> {

   private static final Log log = LogFactory.getLog(AbstractJdbcStoreConfigurationBuilder.class, Log.class);
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
    * Use the specified {@link ConnectionFactory} to handle connection to the database
    */
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
   public <C extends ConnectionFactoryConfigurationBuilder<?>> C connectionFactory(C builder) {
      if (connectionFactory != null) {
         throw new IllegalStateException("A ConnectionFactory has already been configured for this store");
      }
      this.connectionFactory = (ConnectionFactoryConfigurationBuilder<ConnectionFactoryConfiguration>) builder;
      return builder;
   }

   public S manageConnectionFactory(boolean manageConnectionFactory) {
      attributes.attribute(MANAGE_CONNECTION_FACTORY).set(manageConnectionFactory);
      return self();
   }

   public S dialect(DatabaseType databaseType) {
      attributes.attribute(DIALECT).set(databaseType);
      return self();
   }

   @Override
   public void validate() {
      super.validate();
      boolean manageConnectionFactory = attributes.attribute(MANAGE_CONNECTION_FACTORY).get();
      if (manageConnectionFactory && connectionFactory == null) {
         throw log.missingConnectionFactory();
      } else if (!manageConnectionFactory && connectionFactory != null) {
         throw log.unmanagedConnectionFactory();
      }

      if (connectionFactory != null) {
         connectionFactory.validate();
      }
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
