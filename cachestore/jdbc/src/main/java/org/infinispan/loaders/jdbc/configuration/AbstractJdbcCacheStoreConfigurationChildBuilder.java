package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.configuration.cache.AbstractLockSupportStoreConfigurationChildBuilder;

/**
 * AbstractJdbcCacheStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractJdbcCacheStoreConfigurationChildBuilder<S extends AbstractJdbcCacheStoreConfigurationBuilder<?, S>>
      extends AbstractLockSupportStoreConfigurationChildBuilder<S> implements JdbcCacheStoreConfigurationChildBuilder<S> {

   private AbstractJdbcCacheStoreConfigurationBuilder<?, S> builder;

   protected AbstractJdbcCacheStoreConfigurationChildBuilder(AbstractJdbcCacheStoreConfigurationBuilder<?, S> builder) {
      super(builder);
      this.builder = builder;
   }

   @Override
   public PooledConnectionFactoryConfigurationBuilder<S> connectionPool() {
      return builder.connectionPool();
   }

   @Override
   public ManagedConnectionFactoryConfigurationBuilder<S> dataSource() {
      return builder.dataSource();
   }

   @Override
   public SimpleConnectionFactoryConfigurationBuilder<S> simpleConnection() {
      return builder.simpleConnection();
   }

}
