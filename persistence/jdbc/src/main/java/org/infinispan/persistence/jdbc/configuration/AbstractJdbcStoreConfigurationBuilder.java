package org.infinispan.persistence.jdbc.configuration;

import java.lang.reflect.Constructor;

import org.infinispan.commons.configuration.ConfigurationUtils;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;

public abstract class AbstractJdbcStoreConfigurationBuilder<T extends AbstractJdbcStoreConfiguration, S extends AbstractJdbcStoreConfigurationBuilder<T, S>> extends
                                                                                                                                                                                  AbstractStoreConfigurationBuilder<T, S> implements JdbcStoreConfigurationChildBuilder<S> {

   private static final Log log = LogFactory.getLog(AbstractJdbcStoreConfigurationBuilder.class, Log.class);
   protected ConnectionFactoryConfigurationBuilder<ConnectionFactoryConfiguration> connectionFactory;
   protected boolean manageConnectionFactory = true;

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

   @Override
   public void validate() {
      super.validate();
      if (manageConnectionFactory && connectionFactory == null) {
         throw log.missingConnectionFactory();
      } else if (!manageConnectionFactory && connectionFactory != null) {
         throw log.unmanagedConnectionFactory();
      }
   }

   /*
    * TODO: we should really be using inheritance here, but because of a javac bug it won't let me
    * invoke super.read() from subclasses complaining that abstract methods cannot be invoked. Will
    * open a bug and add the ID here
    */
   protected S readInternal(AbstractJdbcStoreConfiguration template) {
      Class<? extends ConnectionFactoryConfigurationBuilder<?>> cfb = (Class<? extends ConnectionFactoryConfigurationBuilder<?>>) ConfigurationUtils.builderFor(template.connectionFactory());
      connectionFactory(cfb);
      connectionFactory.read(template.connectionFactory());
      manageConnectionFactory = template.manageConnectionFactory();

      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      this.async.read(template.async());
      this.singletonStore.read(template.singletonStore());
      this.preload = template.preload();
      this.shared = template.shared();

      return self();
   }
}
