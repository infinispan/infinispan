package org.infinispan.persistence.jdbc.configuration;

import java.lang.reflect.Constructor;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationUtils;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.CacheConfigurationException;
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
   protected boolean manageConnectionFactory = true;
   protected DatabaseType databaseType;

   public AbstractJdbcStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
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
    * Use the specified {@link ConnectionFactoryConfigurationBuilder} to configure connections to the database
    */
   public <C extends ConnectionFactoryConfigurationBuilder<?>> C connectionFactory(C builder) {
      if (connectionFactory != null) {
         throw new IllegalStateException("A ConnectionFactory has already been configured for this store");
      }
      this.connectionFactory = (ConnectionFactoryConfigurationBuilder<ConnectionFactoryConfiguration>) builder;
      return builder;
   }

   public S manageConnectionFactory(boolean manageConnectionFactory) {
      this.manageConnectionFactory = manageConnectionFactory;
      return self();
   }

   public S dialect(DatabaseType databaseType) {
      this.databaseType = databaseType;
      return self();
   }

   @Override
   public void validate() {
      super.validate();
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
      Class<? extends ConnectionFactoryConfigurationBuilder<?>> cfb = (Class<? extends ConnectionFactoryConfigurationBuilder<?>>) ConfigurationUtils.builderFor(template.connectionFactory());
      connectionFactory(cfb);
      connectionFactory.read(template.connectionFactory());
      manageConnectionFactory = template.manageConnectionFactory();
      databaseType = template.dialect();

      return super.read(template);
   }
}
